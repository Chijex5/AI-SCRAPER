import asyncio
import json
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from enum import Enum
from pymongo import ReturnDocument
from typing import Any, Optional
from datetime import datetime, timedelta, timezone   # ← add timedelta
import hashlib                      
from health import router as health_router           
import httpx
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from bs4 import BeautifulSoup
from bson import ObjectId
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection
from pydantic import BaseModel, ValidationError
from google import genai
from google.genai import types as genai_types
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception
from telethon import TelegramClient
from telethon.errors import (
    ChannelInvalidError,
    UsernameNotOccupiedError,
    AuthKeyError,
    AuthKeyUnregisteredError,
    SessionExpiredError,
    SessionRevokedError,
    UserDeactivatedError,
    UserDeactivatedBanError,
    SessionPasswordNeededError,
)
from telethon.sessions import StringSession
from score import score_signal, SENIORITY_KEYWORDS, SENIOR_KEYWORDS


load_dotenv()

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CONFIG
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
MONGODB_URI = os.getenv("MONGODB_URI", "")
DB_NAME     = os.getenv("DB_NAME", "jobless")

SCRAPE_HOUR   = int(os.getenv("SCRAPE_HOUR",   "0"))
SCRAPE_MINUTE = int(os.getenv("SCRAPE_MINUTE", "0"))

TELEGRAM_API_ID   = int(os.getenv("TELEGRAM_API_ID",   "0"))
TELEGRAM_API_HASH = os.getenv("TELEGRAM_API_HASH",     "")
TELEGRAM_PHONE    = os.getenv("TELEGRAM_PHONE",        "")
SESSION_STRING = os.getenv("TELEGRAM_SESSION_STRING", "")


BATCH_SIZE = 8   # smaller — richer prompt = more tokens per item

# ── Gemini key pool ───────────────────────────────────────────────────────────
_raw_keys: list[str] = [
    os.getenv("GEMINI_API_KEY_1", os.getenv("GEMINI_API_KEY", "")),
    os.getenv("GEMINI_API_KEY_2", ""),
    os.getenv("GEMINI_API_KEY_3", ""),
]
GEMINI_KEYS: list[str] = [k.strip() for k in _raw_keys if k.strip()]

if not GEMINI_KEYS:
    raise RuntimeError("At least one Gemini API key is required (GEMINI_API_KEY_1 or GEMINI_API_KEY).")

_gemini_clients: list[genai.Client] = [genai.Client(api_key=k) for k in GEMINI_KEYS]
_client_index = 0

print(f"🔑 Gemini pool: {len(_gemini_clients)} key(s) loaded")

# ── Scoring / ranking maps ────────────────────────────────────────────────────
EXTRACTION_CONFIDENCE_SCORE: dict[str, int] = {"High": 90, "Medium": 65, "Low": 40}

LOCATION_RANK: dict[str, int] = {
    "Nigeria Remote": 0,
    "Nigeria Onsite": 1,
    "Remote":         2,
    "Other":          3,
    "Unknown":        4,
}




# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# PYDANTIC MODELS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class ApplicationStatus(str, Enum):
    open         = "Open"
    closing_soon = "Closing soon"
    unknown      = "Unknown"


class RoleMode(str, Enum):
    remote  = "Remote"
    hybrid  = "Hybrid"
    onsite  = "On-site"


class ConfidenceLevel(str, Enum):
    high   = "High"
    medium = "Medium"
    low    = "Low"


class PipelineStage(str, Enum):
    new          = "new"
    saved        = "saved"
    applied      = "applied"
    interviewing = "interviewing"
    offered      = "offered"
    rejected     = "rejected"


class IntelligenceSignal(BaseModel):
    """The primary response shape — mirrors the TypeScript type."""
    id:                   str
    role:                 str
    company:              str
    location:             str
    aiMatchScore:         int
    postedAt:             str
    aiSummary:            str
    skillTags:            list[str]
    sourcePostPreview:    str
    sourceHandle:         str
    sourceUrl:            str
    relevanceReason:      str
    skillAlignment:       str
    extractionConfidence: ConfidenceLevel
    roleType:             str
    roleMode:             RoleMode
    applicationStatus:    ApplicationStatus
    sourceConfidence:     ConfidenceLevel
    originalSourceText:   str
    sourceMetadata:       list[str]
    relatedIds:           list[str]
    # Pipeline extras (not in the TS type but useful for the app)
    platform:             str
    status:               PipelineStage
    addedAt:              str
    isSaved:             bool = False
    isIntearested:         bool = True
    isSkipped:           bool = False
    pay:                  Optional[str] = None
    applyLink:            Optional[str] = None


class StatusUpdate(BaseModel):
    status: PipelineStage


class PaginatedSignals(BaseModel):
    signals: list[IntelligenceSignal]
    total:   int
    page:    int
    pages:   int
    limit:   int


class ScrapeStatus(BaseModel):
    running:       bool
    lastRun:       Optional[str] = None
    lastSaved:     Optional[int] = None
    nextRun:       Optional[str] = None
    message:       str
    progress:      int           = 0
    phase:         str           = "idle"
    currentSource: Optional[str] = None

class NotificationCategory(str):
    """Mirrors the TypeScript union — kept as str so it round-trips cleanly."""
    HIGH_MATCH   = "High Match Opportunity"
    TRENDING     = "Trending Opportunity"
    DEADLINE     = "Deadline Alert"
    AI_INSIGHT   = "AI Insight Alert"
    SYSTEM       = "System Activity"
    QUEUE        = "Queue Reminder"
 
 
class NotificationItemModel(BaseModel):
    id:           str
    section:      str
    title:        str
    context:      str
    time:         str
    category:     str
    priority:     str
    matchScore:   Optional[int]  = None
    urgency:      Optional[str]  = None
    actions:      list[str]
    signalId:     Optional[str]  = None
    generatedAt:  str
    # ── user-persisted state (new in v2) ─────────────────────────────────────
    isImportant:  bool           = False   # toggled by user, survives regeneration
    isDismissed:  bool           = False   # soft-delete; restore sets back to False

 
class TelegramChannel(BaseModel):
    id:        str
    handle:    str          # e.g. "jobnetworkng"  (no @)
    name:      str          # display name
    active:    bool = True  # enable/disable scraping
    addedAt:   str
 
 
class TelegramChannelCreate(BaseModel):
    handle: str             # user supplies this — we normalise the @
    name:   Optional[str] = None
 
 
class TelegramChannelPatch(BaseModel):
    active: Optional[bool] = None
    name:   Optional[str]  = None

class PipelineEvent(BaseModel):
    id:      str
    message: str
    status:  str   # active | info | high | error
    at:      str   # ISO timestamp
 
 
class MonitorResponse(BaseModel):
    # ── Scrape status ─────────────────────────────────────────────────────
    scrapeRunning:    bool
    scrapePhase:      str
    scrapeProgress:   int
    currentSource:    Optional[str]
    lastRun:          Optional[str]
    lastSaved:        Optional[int]
    nextRun:          Optional[str]
 
    # ── Signal counts ─────────────────────────────────────────────────────
    totalSignals:     int
    newSignals:       int          # status == "new"
    savedSignals:     int          # isSaved == True
    highMatchSignals: int          # aiMatchScore >= 80
 
    # ── Platform breakdown ────────────────────────────────────────────────
    platformCounts:   dict[str, int]   # { "Telegram": 42, "Remotive": 18, ... }
 
    # ── Notification stats ────────────────────────────────────────────────
    notifRunning:     bool
    notifLastRun:     Optional[str]
    notifLastCount:   Optional[int]
 
    # ── Live events ───────────────────────────────────────────────────────
    recentEvents:     list[PipelineEvent]

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# DATABASE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class Database:
    client: AsyncIOMotorClient | None = None
    signals: AsyncIOMotorCollection | None = None


db = Database()


async def connect_db() -> None:
    db.client = AsyncIOMotorClient(MONGODB_URI)
    col = db.client[DB_NAME]["signals"]
    await col.create_index("scrapedId", unique=True)
    await col.create_index("platform")
    await col.create_index("location")
    await col.create_index("roleMode")
    await col.create_index("roleType")
    await col.create_index("applicationStatus")
    await col.create_index("extractionConfidence")
    await col.create_index("status")
    channels_col = db.client[DB_NAME]["telegram_channels"]
    await channels_col.create_index("handle", unique=True)
    await channels_col.create_index("active")
    await seed_channels_if_empty()
    db.signals = col
    notif_col = db.client[DB_NAME]["notifications"]
    await notif_col.create_index("section")
    await notif_col.create_index("priority")
    await notif_col.create_index("isRead")
    await notif_col.create_index("generatedAt")
    # In connect_db(), add this index:
    await col.create_index([
        ("role",      "text"),
        ("company",   "text"),
        ("aiSummary", "text"),
        ("skillTags", "text"),
    ], name="text_search")
    existing_cols = await db.client[DB_NAME].list_collection_names()
    if "pipeline_events" not in existing_cols:
        await db.client[DB_NAME].create_collection(
            "pipeline_events",
            capped=True,
            size=1_000_000,   # 1 MB cap
            max=100,          # max 100 documents
        )
    print("✅ pipeline_events collection ready")
    health_col = db.client[DB_NAME]["source_health"]
    await health_col.create_index([("source", 1), ("at", -1)])
    print(f"✅ MongoDB connected → {DB_NAME}.signals")


async def close_db() -> None:
    if db.client:
        db.client.close()

def get_channels_col():
    if db.client is None:
        raise RuntimeError("Database not initialised")
    return db.client[DB_NAME]["telegram_channels"]
 
 
async def seed_channels_if_empty():
    """On first boot, populate the channels collection from the hardcoded seed list."""
    col = get_channels_col()
    if await col.count_documents({}) == 0:
        now = datetime.now(timezone.utc).isoformat()
        docs = [
            {
                "_id":     handle,
                "id":      handle,
                "handle":  handle,
                "name":    f"@{handle}",
                "active":  True,
                "addedAt": now,
            }
            for handle in TELEGRAM_CHANNELS_SEED
        ]
        await col.insert_many(docs)
        print(f"🌱 Seeded {len(docs)} Telegram channels into DB")
 
 
async def get_active_channels() -> list[str]:
    """Returns handles of all enabled channels."""
    col = get_channels_col()
    docs = await col.find({"active": True}, {"handle": 1}).to_list(length=500)
    return [d["handle"] for d in docs]

def get_signals() -> AsyncIOMotorCollection:
    if db.signals is None:
        raise RuntimeError("Database not initialised")
    return db.signals


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SCRAPE STATE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
scrape_state: dict = {
    "running":          False,
    "last_run":         None,
    "last_saved":       None,
    # ── progress fields (new) ─────────────────────────────────────────────
    "progress":         0,       # 0–100 int
    "total_steps":      0,       # total work units for this run
    "completed_steps":  0,       # how many finished
    "current_source":   None,    # e.g. "Telegram @jobnetworkng"
    "phase":            "idle",  # idle | fetching | validating | saving | done
}


notifications_state: dict[str, Any] = {
    "running":    False,
    "last_run":   None,
    "last_count": None,
}
 
 
def get_notifications_col() -> AsyncIOMotorCollection:
    if db.client is None:
        raise RuntimeError("Database not initialised")
    return db.client[DB_NAME]["notifications"]

def get_events_col():
    if db.client is None:
        raise RuntimeError("Database not initialised")
    return db.client[DB_NAME]["pipeline_events"]

async def log_event(message: str, status: str = "info") -> None:
    """Insert one pipeline event. Silently swallows errors so it never breaks the pipeline."""
    try:
        col = get_events_col()
        await col.insert_one({
            "message": message,
            "status":  status,
            "at":      datetime.now(timezone.utc).isoformat(),
        })
    except Exception as e:
        print(f"  ⚠  log_event failed: {e}")


def get_source_health_col():
    if db.client is None:
        raise RuntimeError("Database not initialised")
    return db.client[DB_NAME]["source_health"]


_SOURCE_HEALTH_LOOKBACK = 7   # runs to average over when judging a drop


async def record_source_health(source: str, count: int) -> None:
    """
    Record this run's listing count for `source` and flag an unexpected drop
    against the rolling average of its last few runs (e.g. a site redesign
    silently breaking a BeautifulSoup selector). Never raises.
    """
    try:
        col = get_source_health_col()
        recent = await col.find(
            {"source": source}, {"count": 1, "_id": 0}
        ).sort("at", -1).limit(_SOURCE_HEALTH_LOOKBACK).to_list(length=_SOURCE_HEALTH_LOOKBACK)

        await col.insert_one({
            "source": source,
            "count":  count,
            "at":     datetime.now(timezone.utc).isoformat(),
        })

        if recent:
            avg = sum(d["count"] for d in recent) / len(recent)
            if avg >= 3 and count == 0:
                await log_event(
                    f"⚠ Source health: {source} returned 0 listings "
                    f"(recent avg {avg:.1f} over {len(recent)} runs) — possible breakage",
                    "error",
                )
            elif avg >= 3 and count <= avg * 0.1:
                await log_event(
                    f"⚠ Source health: {source} returned {count} listings, "
                    f"a sharp drop from recent avg {avg:.1f}",
                    "error",
                )
    except Exception as e:
        print(f"  ⚠  record_source_health failed: {e}")

def _notif_id(prefix: str, *parts: str) -> str:
    """Stable, short, collision-resistant ID for a notification."""
    raw = f"{prefix}:{'|'.join(str(p) for p in parts)}"
    return hashlib.md5(raw.encode()).hexdigest()[:16]
 
 
def _fmt_time(iso_str: str, now: datetime) -> str:
    """
    Format an ISO timestamp as a human-readable relative time string that
    matches the signal.tsx display:
      • same day   →  "9:14 AM"
      • this week  →  "Mon · 4:42 PM"
      • older      →  "May 20"
    """
    if not iso_str:
        return ""
    try:
        dt = datetime.fromisoformat(iso_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        local_dt = dt.astimezone()
        delta    = now - dt
 
        h    = local_dt.hour % 12 or 12
        mins = local_dt.strftime("%M")
        ampm = "AM" if local_dt.hour < 12 else "PM"
        t    = f"{h}:{mins} {ampm}"
 
        if delta.total_seconds() < 0:          # future timestamp guard
            return t
        if delta.days == 0:
            return t
        if delta.days <= 7:
            return f"{local_dt.strftime('%a')} · {t}"
        return local_dt.strftime("%b %d")
    except Exception:
        return ""
 
 
def _clean(text: str, max_len: int = 200) -> str:
    """Strip whitespace, collapse newlines, truncate."""
    return " ".join(text.split())[:max_len].strip()
 
 
async def _generate_ai_insight(signals: list[dict], now: datetime) -> dict:
    """
    Ask Gemini to generate ONE concise trend insight from the week's aggregate
    signal data. Falls back to a computed insight if Gemini is unavailable.
 
    Returns {"title": str, "context": str}
    """
    # ── Aggregate stats ───────────────────────────────────────────────────────
    role_counts:     dict[str, int] = {}
    location_counts: dict[str, int] = {}
    tag_counts:      dict[str, int] = {}
    platforms:       set[str]       = set()
 
    for s in signals:
        rt  = s.get("roleType",  "Software Engineering")
        loc = s.get("location",  "Unknown")
        plt = s.get("platform",  "Unknown")
 
        role_counts[rt]     = role_counts.get(rt, 0)     + 1
        location_counts[loc]= location_counts.get(loc, 0)+ 1
        platforms.add(plt)
 
        for tag in s.get("skillTags", []):
            if tag:
                tag_counts[tag] = tag_counts.get(tag, 0) + 1
 
    top_roles    = sorted(role_counts.items(),     key=lambda x: -x[1])[:5]
    top_tags     = sorted(tag_counts.items(),      key=lambda x: -x[1])[:6]
    top_locs     = sorted(location_counts.items(), key=lambda x: -x[1])[:4]
    remote_frac  = round(
        (location_counts.get("Remote", 0) + location_counts.get("Nigeria Remote", 0))
        / max(len(signals), 1) * 100
    )
 
    stats = {
        "total_signals_this_week": len(signals),
        "top_role_types":          top_roles,
        "top_skill_tags":          top_tags,
        "top_locations":           top_locs,
        "platforms":               sorted(platforms),
        "remote_percentage":       remote_frac,
    }
 
    prompt = (
        "You are generating a single concise market-intelligence notification "
        "for a Nigerian software developer job-tracking app called Signal.\n\n"
        "From this week's scraped job data, generate ONE insight notification.\n\n"
        f"DATA:\n{json.dumps(stats, indent=2)}\n\n"
        "Return ONLY a JSON object — no markdown fences, no commentary:\n"
        '{\n'
        '  "title": "Insight headline — max 12 words, present tense, cite a concrete number",\n'
        '  "context": "1–2 sentences expanding the insight for a Nigerian dev (max 180 chars)"\n'
        '}\n\n'
        "Example good titles:\n"
        '- "React roles up 21% this week across 4 platforms"\n'
        '- "Remote-first listings dominate — 67% of new opportunities"\n'
        '- "TypeScript appears in 8 of the top 10 frontend roles this week"'
    )
 
    try:
        client, _ = _next_client()
        resp  = client.models.generate_content(model="gemini-2.5-flash", contents=prompt)
        raw   = resp.text.strip()
        if raw.startswith("```"):
            parts = raw.split("```")
            raw   = parts[1][4:] if parts[1].startswith("json") else parts[1]
        data  = json.loads(raw.strip())
        return {
            "title":   data.get("title",   ""),
            "context": data.get("context", ""),
        }
    except Exception as e:
        print(f"  ⚠  AI insight generation failed: {e} — using computed fallback")
 
    # ── Computed fallback (no Gemini needed) ──────────────────────────────────
    top_role_name, top_role_count = top_roles[0] if top_roles else ("Software Engineering", 0)
    top_tag_name                  = top_tags[0][0] if top_tags else "React"
    source_count                  = len(platforms)
 
    return {
        "title":   (
            f"{top_role_name} roles led this week — "
            f"{top_role_count} new listing{'s' if top_role_count != 1 else ''}"
        ),
        "context": (
            f"{top_tag_name} was the most demanded skill across {source_count} "
            f"source{'s' if source_count != 1 else ''}. "
            f"{remote_frac}% of this week's opportunities are remote-friendly."
        ),
    }

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# MAPPER  raw item → MongoDB doc → IntelligenceSignal
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def build_scraped_id(item: dict) -> str:
    return f"{item['source']}::{item['id']}"


def _truncate(text: str, n: int = 300) -> str:
    return text[:n].replace("\n", " ").strip()


def item_to_doc(item: dict) -> dict:
    """
    Merges raw scraped data with Gemini-enriched fields into a flat MongoDB doc.
    All IntelligenceSignal fields are present; unknown ones get safe defaults.
    """
    raw_text  = (item.get("text") or "").strip()
    source    = item.get("source", "Unknown")
    url       = item.get("url", "")
    username  = item.get("username", "")
    channel   = item.get("channel", "")

    # ── Gemini-extracted fields (with defaults if Gemini skipped them) ────────
    role                  = (item.get("position")             or "Unknown").strip()
    company               = (item.get("company")              or "Unknown").strip()
    pay                   = (item.get("pay")                  or "").strip() or None
    location_raw          = (item.get("location")             or "Unknown").strip()
    notes                 = (item.get("notes")                or "").strip()
    apply_link            = (item.get("apply_link")           or url or "").strip()
    skill_tags: list[str] = item.get("skill_tags")            or []
    relevance_reason      = (item.get("relevance_reason")     or "").strip()
    skill_alignment       = (item.get("skill_alignment")      or "").strip()
    role_type             = (item.get("role_type")            or "Software Engineering").strip()
    role_mode_raw         = (item.get("role_mode")            or "Unknown").strip()
    app_status_raw        = (item.get("application_status")   or "Unknown").strip()
    source_confidence_raw = (item.get("source_confidence")    or "Medium").strip()
    extraction_conf_raw   = (item.get("confidence")           or "Medium").strip()

    # ── Normalise enums ───────────────────────────────────────────────────────
    role_mode_map = {
        "remote": "Remote", "hybrid": "Hybrid",
        "on-site": "On-site", "onsite": "On-site", "on site": "On-site",
    }
    role_mode = role_mode_map.get(role_mode_raw.lower(), "Remote")

    app_status_map = {
        "open": "Open", "closing soon": "Closing soon",
        "closing": "Closing soon", "unknown": "Unknown",
    }
    application_status = app_status_map.get(app_status_raw.lower(), "Unknown")

    def _conf(raw: str) -> str:
        return {"high": "High", "medium": "Medium", "low": "Low"}.get(raw.lower(), "Medium")

    extraction_confidence = _conf(extraction_conf_raw)
    source_confidence     = _conf(source_confidence_raw)

    # ── Derived / computed fields ─────────────────────────────────────────────
    ai_summary        = notes or _truncate(raw_text, 220) or "No summary available."
    source_post_prev  = _truncate(raw_text, 280)
    source_handle     = channel or username or source
    source_metadata   = [s for s in [source, channel, username, location_raw] if s and s != "Unknown"]

    doc = {
        # ── Identity ──────────────────────────────────────────────────────────
        "scrapedId":           build_scraped_id(item),
        "platform":            source,
        "status":              PipelineStage.new.value,
        "addedAt":             datetime.now(timezone.utc).isoformat(),

        # ── IntelligenceSignal fields ─────────────────────────────────────────
        "role":                role,
        "company":             company,
        "location":            location_raw,
        "aiMatchScore":        0,          # set below, after the doc is assembled
        "postedAt":            str(item.get("created_at") or ""),
        "aiSummary":           ai_summary,
        "skillTags":           skill_tags,
        "sourcePostPreview":   source_post_prev,
        "sourceHandle":        source_handle,
        "sourceUrl":           url,
        "relevanceReason":     relevance_reason,
        "skillAlignment":      skill_alignment,
        "extractionConfidence": extraction_confidence,
        "roleType":            role_type,
        "roleMode":            role_mode,
        "applicationStatus":   application_status,
        "sourceConfidence":    source_confidence,
        "originalSourceText":  raw_text,
        "sourceMetadata":      source_metadata,
        "relatedIds":          [],          # manual curation only; GET /related computes matches on read

        # ── Extras ────────────────────────────────────────────────────────────
        "pay":       pay,
        "applyLink": apply_link,
    }

    # Score the fully-normalised doc (DB field names) — NOT the raw Gemini item,
    # whose keys (position/role_mode/notes/created_at/skill_tags…) don't match
    # what score_signal reads, which silently zeroed out most components.
    doc["aiMatchScore"], _ = score_signal(doc)
    return doc


def doc_to_signal(doc: dict) -> IntelligenceSignal:
    return IntelligenceSignal(
        id                   = str(doc["_id"]),
        role                 = doc.get("role",                "Unknown"),
        company              = doc.get("company",             "Unknown"),
        location             = doc.get("location",            "Unknown"),
        aiMatchScore         = doc.get("aiMatchScore",        50),
        postedAt             = doc.get("postedAt",            ""),
        aiSummary            = doc.get("aiSummary",           ""),
        skillTags            = doc.get("skillTags",           []),
        sourcePostPreview    = doc.get("sourcePostPreview",   ""),
        sourceHandle         = doc.get("sourceHandle",        ""),
        sourceUrl            = doc.get("sourceUrl",           ""),
        relevanceReason      = doc.get("relevanceReason",     ""),
        skillAlignment       = doc.get("skillAlignment",      ""),
        extractionConfidence = doc.get("extractionConfidence","Medium"),
        roleType             = doc.get("roleType",            "Software Engineering"),
        roleMode             = doc.get("roleMode",            "Remote"),
        applicationStatus    = doc.get("applicationStatus",   "Unknown"),
        sourceConfidence     = doc.get("sourceConfidence",    "Medium"),
        originalSourceText   = doc.get("originalSourceText",  ""),
        sourceMetadata       = doc.get("sourceMetadata",      []),
        relatedIds           = doc.get("relatedIds",          []),
        platform             = doc.get("platform",            "Unknown"),
        status               = doc.get("status",              PipelineStage.new),
        addedAt              = doc.get("addedAt",             ""),

        # MISSING FIELDS
        isSaved              = doc.get("isSaved", False),
        isIntearested        = doc.get("isIntearested", True),
        isSkipped            = doc.get("isSkipped", False),

        pay                  = doc.get("pay"),
        applyLink            = doc.get("applyLink"),
    )
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SCRAPERS  (unchanged from original)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ROLE_KEYWORDS = [
    "frontend", "front-end", "front end",
    "backend",  "back-end",  "back end",
    "fullstack", "full-stack", "full stack",
    "mobile", "android", "ios", "react native", "flutter",
    "intern", "internship", "junior", "entry",
    "developer", "engineer", "programmer",
]

TELEGRAM_CHANNELS_SEED = [
    "jobnetworkng", "remotejobss", "ingressive4good",
    "jbtoday", "nigeriatechjobs", "lagostechjobs",
    "techJobsNG", "devjobsng", "africatechjobs", "remotejobsafrica",
]

TELEGRAM_ROLE_KEYWORDS = [
    "intern", "internship", "junior", "entry level", "entry-level",
    "frontend", "front-end", "backend", "back-end",
    "fullstack", "full stack", "full-stack",
    "mobile developer", "react native", "flutter",
    "android developer", "ios developer",
    "software developer", "software engineer",
    "we are hiring", "we're hiring", "now hiring",
    "open role", "open position", "job opening",
    "apply", "application",
]

BLUESKY_QUERIES = [
    "#NigeriaJobs #hiring",
    "#RemoteJobs developer Nigeria",
    "#TechJobs #hiring remote",
    "#hiring #javascript remote",
    "#hiring #python remote",
    "#hiring #react remote",
    "#softwareengineer remote #hiring",
    "#Hiring software engineer Africa",
]


def _is_retryable_http_error(exc: BaseException) -> bool:
    """Retry on timeouts/connection errors and 5xx — not on 4xx (won't fix itself)."""
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code >= 500
    return isinstance(exc, (httpx.TimeoutException, httpx.TransportError))


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=8),
    retry=retry_if_exception(_is_retryable_http_error),
    reraise=True,
)
async def _get(client: httpx.AsyncClient, url: str, **kwargs) -> httpx.Response:
    """Shared GET with retry-with-backoff for transient failures."""
    resp = await client.get(url, **kwargs)
    resp.raise_for_status()
    return resp


async def fetch_remotive(client: httpx.AsyncClient) -> list[dict]:
    listings: list[dict] = []
    seen: set[str] = set()
    for category in ["software-dev", "mobile"]:
        try:
            resp = await _get(
                client,
                "https://remotive.com/api/remote-jobs",
                params={"category": category, "limit": 50},
                timeout=15,
            )
            for job in resp.json().get("jobs", []):
                jid = str(job.get("id", ""))
                if jid in seen:
                    continue
                combined = (
                    f"{job.get('job_title','')} "
                    f"{' '.join(job.get('tags') or [])} "
                    f"{job.get('description','')}"
                ).lower()
                if not any(kw in combined for kw in ROLE_KEYWORDS):
                    continue
                seen.add(jid)
                listings.append({
                    "id": jid, "source": "Remotive",
                    "text": (
                        f"{job.get('job_title')} at {job.get('company_name')}\n"
                        f"Location: {job.get('candidate_required_location', 'Remote')}\n"
                        f"Salary: {job.get('salary') or 'Not stated'}\n"
                        f"Tags: {', '.join(job.get('tags') or [])}\n"
                        f"URL: {job.get('url', '')}"
                    ),
                    "created_at": job.get("publication_date", ""),
                    "url":        job.get("url", ""),
                    "user":       job.get("company_name", "Unknown"),
                    "username":   job.get("company_name", "unknown"),
                })
        except Exception as e:
            print(f"  ⚠  Remotive [{category}]: {e}")
    print(f"  ↳ Remotive: {len(listings)}")
    return listings


async def fetch_jobberman(client: httpx.AsyncClient) -> list[dict]:
    listings: list[dict] = []
    seen: set[str] = set()
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
    }
    for query in [
        "frontend internship", "backend internship",
        "fullstack internship", "mobile developer internship",
        "software developer internship", "junior developer",
    ]:
        slug = query.replace(" ", "+")
        url  = f"https://www.jobberman.com/jobs?q={slug}&l=Nigeria"
        try:
            resp  = await _get(client, url, headers=headers, timeout=20, follow_redirects=True)
            soup  = BeautifulSoup(resp.text, "html.parser")
            cards = (
                soup.select("div[class*='listing-item']")
                or soup.select("li[class*='job-card']")
                or soup.select("article[class*='job']")
            )
            targets = cards or soup.select("a[href*='/jobs/']")
            for el in targets[:10]:
                if el.name == "a":
                    href, title, company = el.get("href", ""), el.get_text(strip=True), "Jobberman"
                else:
                    link_el  = el.select_one("a")
                    href     = link_el.get("href", "") if link_el else ""
                    title_el = el.select_one("h2, h3, [class*='title']")
                    comp_el  = el.select_one("[class*='company']")
                    title    = title_el.get_text(strip=True) if title_el else ""
                    company  = comp_el.get_text(strip=True)  if comp_el  else "Unknown"
                if not title or len(title) < 5 or href in seen:
                    continue
                seen.add(href)
                full_url = href if href.startswith("http") else f"https://www.jobberman.com{href}"
                listings.append({
                    "id": href, "source": "Jobberman",
                    "text": f"{title} at {company}\nLocation: Nigeria\nURL: {full_url}",
                    "created_at": "", "url": full_url,
                    "user": company, "username": "jobberman",
                })
        except Exception as e:
            print(f"  ⚠  Jobberman [{query}]: {e}")
        await asyncio.sleep(2)
    print(f"  ↳ Jobberman: {len(listings)}")
    return listings


async def fetch_myjobmag(client: httpx.AsyncClient) -> list[dict]:
    listings: list[dict] = []
    seen: set[str] = set()
    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}
    for query in [
        "frontend developer", "backend developer", "software developer",
        "mobile developer", "junior developer", "software intern",
    ]:
        slug = query.replace(" ", "+")
        url  = f"https://www.myjobmag.com/search-jobs?keywords={slug}&location=Nigeria"
        try:
            resp  = await _get(client, url, headers=headers, timeout=20, follow_redirects=True)
            soup  = BeautifulSoup(resp.text, "html.parser")
            cards = (
                soup.select("div.job-list-item")
                or soup.select("li.job-item")
                or soup.select("article[class*='job']")
            )
            targets = cards or soup.select("a[href*='/job/']")
            for el in targets[:10]:
                if el.name == "a":
                    href, title = el.get("href", ""), el.get_text(strip=True)
                else:
                    link_el  = el.select_one("a")
                    href     = link_el.get("href", "") if link_el else ""
                    title_el = el.select_one("h2, h3, [class*='title']")
                    title    = title_el.get_text(strip=True) if title_el else el.get_text(strip=True)[:80]
                if not title or len(title) < 5 or not href or href in seen:
                    continue
                seen.add(href)
                full_url = href if href.startswith("http") else f"https://www.myjobmag.com{href}"
                listings.append({
                    "id": href, "source": "MyJobMag",
                    "text": f"{title}\nLocation: Nigeria\nURL: {full_url}",
                    "created_at": "", "url": full_url,
                    "user": "MyJobMag", "username": "myjobmag",
                })
        except Exception as e:
            print(f"  ⚠  MyJobMag [{query}]: {e}")
        await asyncio.sleep(2)
    print(f"  ↳ MyJobMag: {len(listings)}")
    return listings


async def fetch_himalayas(client: httpx.AsyncClient) -> list[dict]:
    listings: list[dict] = []
    seen: set[str] = set()
    for q in [
        "frontend intern", "backend intern", "fullstack intern",
        "mobile developer intern", "junior frontend", "junior backend",
    ]:
        try:
            resp = await _get(
                client,
                "https://himalayas.app/jobs/api",
                params={"q": q, "limit": 20},
                headers={"Accept": "application/json"},
                timeout=15,
            )
            for job in resp.json().get("jobs", []):
                jid = str(job.get("id") or job.get("slug", ""))
                if jid in seen:
                    continue
                seen.add(jid)
                slug = job.get("slug", "")
                listings.append({
                    "id": jid, "source": "Himalayas",
                    "text": (
                        f"{job.get('title')} at {job.get('company', {}).get('name', 'Unknown')}\n"
                        f"Location: {job.get('locationRestrictions') or 'Remote'}\n"
                        f"Salary: {job.get('salaryRange') or 'Not stated'}\n"
                        f"URL: https://himalayas.app/jobs/{slug}"
                    ),
                    "created_at": job.get("createdAt", ""),
                    "url": f"https://himalayas.app/jobs/{slug}",
                    "user": job.get("company", {}).get("name", "Unknown"),
                    "username": "himalayas",
                })
        except Exception as e:
            print(f"  ⚠  Himalayas [{q}]: {e}")
        await asyncio.sleep(1)
    print(f"  ↳ Himalayas: {len(listings)}")
    return listings


async def fetch_remoteok(client: httpx.AsyncClient) -> list[dict]:
    listings: list[dict] = []
    seen: set[str] = set()
    try:
        resp = await _get(
            client,
            "https://remoteok.com/api",
            headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"},
            timeout=20,
        )
        jobs = resp.json()
        for job in jobs[1:]:  # index 0 is a legal header object
            jid = str(job.get("id", ""))
            if not jid or jid in seen:
                continue
            tags = job.get("tags") or []
            position = job.get("position", "")
            description = BeautifulSoup(job.get("description", ""), "html.parser").get_text()
            combined = f"{position} {' '.join(tags)} {description}".lower()
            if not any(kw in combined for kw in ROLE_KEYWORDS):
                continue
            seen.add(jid)
            listings.append({
                "id":         jid,
                "source":     "RemoteOK",
                "text": (
                    f"{position} at {job.get('company', 'Unknown')}\n"
                    f"Location: Remote\n"
                    f"Tags: {', '.join(tags)}\n"
                    f"{description[:400]}"
                ),
                "created_at": job.get("date", ""),
                "url":        job.get("url", ""),
                "user":       job.get("company", "Unknown"),
                "username":   "remoteok",
            })
    except Exception as e:
        print(f"  ⚠  RemoteOK: {e}")
    print(f"  ↳ RemoteOK: {len(listings)}")
    return listings


async def fetch_bluesky(client: httpx.AsyncClient) -> list[dict]:
    listings: list[dict] = []
    seen: set[str] = set()
    for query in BLUESKY_QUERIES:
        try:
            resp = await _get(
                client,
                "https://public.api.bsky.app/xrpc/app.bsky.feed.searchPosts",
                params={"q": query, "limit": 100},
                headers={"Accept": "application/json"},
                timeout=15,
            )
            for post in resp.json().get("posts", []):
                uri  = post.get("uri", "")
                text = (post.get("record") or {}).get("text", "")
                if not uri or not text or uri in seen:
                    continue
                if not any(kw in text.lower() for kw in TELEGRAM_ROLE_KEYWORDS):
                    continue
                seen.add(uri)
                author  = post.get("author", {})
                handle  = author.get("handle", "bsky.social")
                rkey    = uri.split("/")[-1]
                listings.append({
                    "id":         uri,
                    "source":     "Bluesky",
                    "text":       text[:1000],
                    "created_at": post.get("indexedAt", ""),
                    "url":        f"https://bsky.app/profile/{handle}/post/{rkey}",
                    "user":       author.get("displayName", handle),
                    "username":   handle,
                })
        except Exception as e:
            print(f"  ⚠  Bluesky [{query}]: {e}")
        await asyncio.sleep(0.5)
    print(f"  ↳ Bluesky: {len(listings)}")
    return listings


async def fetch_telegram() -> list[dict]:
    if not (TELEGRAM_API_ID and TELEGRAM_API_HASH and TELEGRAM_PHONE):
        print("  ↳ Telegram: skipped (credentials not set)")
        return []

    if not SESSION_STRING:
        print("  ↳ Telegram: skipped (SESSION_STRING not set — run scraper.py to generate one)")
        return []

    channels = await get_active_channels()
    if not channels:
        print("  ↳ Telegram: no active channels")
        return []

    listings: list[dict] = []
    seen:     set[str]   = set()

    tg = TelegramClient(
        StringSession(SESSION_STRING),
        TELEGRAM_API_ID,
        TELEGRAM_API_HASH,
    )
    try:
        await tg.start(phone=TELEGRAM_PHONE)
    except (
        AuthKeyError,
        AuthKeyUnregisteredError,
        SessionExpiredError,
        SessionRevokedError,
        UserDeactivatedError,
        UserDeactivatedBanError,
        SessionPasswordNeededError,
    ) as auth_err:
        msg = (
            f"⚠ Telegram session is dead ({type(auth_err).__name__}: {auth_err}) — "
            f"re-run scraper.py and update TELEGRAM_SESSION_STRING."
        )
        print(f"  ↳ {msg}")
        await log_event(msg, "error")
        return []
    except Exception as auth_err:
        print(f"  ↳ Telegram: auth failed ({auth_err}) — skipping. Renew TELEGRAM_SESSION_STRING to fix.")
        return []

    try:
        for i, channel in enumerate(channels):
            scrape_state["current_source"] = f"Telegram @{channel}"
            tg_progress = 70 + int((i / max(len(channels), 1)) * 25)
            scrape_state["progress"] = tg_progress

            try:
                entity   = await tg.get_entity(channel)
                messages = await tg.get_messages(entity, limit=100)
                found    = 0
                for msg in messages:
                    if not msg.text:
                        continue
                    if not any(kw in msg.text.lower() for kw in TELEGRAM_ROLE_KEYWORDS):
                        continue
                    mid = f"{channel}_{msg.id}"
                    if mid in seen:
                        continue
                    seen.add(mid)
                    found += 1
                    listings.append({
                        "id":         mid,
                        "source":     "Telegram",
                        "channel":    channel,
                        "text":       msg.text[:1000],
                        "created_at": msg.date.isoformat() if msg.date else "",
                        "url":        f"https://t.me/{channel}/{msg.id}",
                        "user":       channel,
                        "username":   channel,
                    })
                print(f"    • @{channel}: {found}")
            except (ChannelInvalidError, UsernameNotOccupiedError):
                print(f"    ⚠  @{channel}: not found / private")
            except Exception as e:
                print(f"    ⚠  @{channel}: {e}")
    finally:
        await tg.disconnect()

    print(f"  ↳ Telegram: {len(listings)}")
    return listings

async def scrape_all() -> list[dict]:
    async with httpx.AsyncClient() as client:
        web = await asyncio.gather(
            fetch_remotive(client),
            fetch_jobberman(client),
            fetch_myjobmag(client),
            fetch_himalayas(client),
            fetch_remoteok(client),
            fetch_bluesky(client),
        )
    tg = await fetch_telegram()
    combined: list[dict] = []
    seen: set[str] = set()
    for source_list in [*web, tg]:
        for item in source_list:
            uid = f"{item['source']}:{item['id']}"
            if uid not in seen:
                seen.add(uid)
                combined.append(item)
    return combined


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# GEMINI VALIDATION  —  now extracts the full IntelligenceSignal shape
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
GEMINI_PROMPT = """\
You are an expert technical recruiter filtering and enriching raw job listings for Nigerian software developers.

TASK: Analyse each listing and return a JSON array — one object per listing.

━━ VALIDITY RULES ━━
KEEP (is_valid = true):
  ✅ Any software/tech role — frontend, backend, fullstack, mobile, DevOps, data, QA, ML, etc.
  ✅ Any seniority — intern, junior, mid, senior all count
  ✅ Remote-first global roles count even if no Nigeria mention
  ✅ Telegram posts with vague details but clear hiring intent

DISCARD (is_valid = false) ONLY if:
  ❌ Purely non-tech: pure sales, marketing, accounting, admin, video editing
  ❌ Not a job at all: error page, nav link, "Post a Job" CTA, site content
  ❌ Obvious spam or scam

━━ FIELD GUIDE ━━
confidence (extractionConfidence):
  "High"   = internship / junior / entry-level / graduate — clear match for fresh devs
  "Medium" = real tech role, mid or senior level
  "Low"    = real job but thin details or unclear seniority

source_confidence: how trustworthy is the source?
  "High"   = well-known job board or verified company page
  "Medium" = smaller board, aggregator, or Telegram channel
  "Low"    = unverified, anonymous, or suspicious

role_mode: "Remote" | "Hybrid" | "On-site"
application_status: "Open" | "Closing soon" | "Unknown"
  Set "Closing soon" if a deadline is within 7 days of today.

skill_tags: extract 3–8 specific technology/skill keywords (e.g. "React", "Node.js", "TypeScript", "REST API")
role_type: one of "Software Engineering" | "Mobile" | "Data" | "DevOps" | "QA" | "Design" | "Other"
location: "Nigeria Remote" | "Nigeria Onsite" | "Remote" | "Other" | "Unknown"

relevance_reason: 1–2 sentence explanation of why this role is relevant to a Nigerian dev job-seeker
skill_alignment: comma-separated list of skills from the listing that match common junior-dev skill sets
                 (e.g. "React, Node.js, Git") — empty string if none detectable

notes: concise 1–2 sentence summary suitable for an AI feed card (aiSummary)
pay: exact pay/stipend string from the listing, or null
apply_link: direct application URL, or null

━━ OUTPUT ━━
One object per listing, indexed from 0. "reason" is a one-sentence note if
is_valid is false, else an empty string.

LISTINGS:
"""


class GeminiListingResult(BaseModel):
    """
    Schema enforced on Gemini's output via response_schema — the model is
    constrained to emit valid JSON matching this shape, which removes the
    need to strip markdown fences / guess at malformed output.
    """
    index:              int
    is_valid:           bool
    confidence:         str            = "Medium"
    source_confidence:  str            = "Medium"
    reason:             str            = ""
    company:            Optional[str]  = None
    position:           Optional[str]  = None
    pay:                Optional[str]  = None
    location:           str            = "Unknown"
    role_mode:          str            = "Remote"
    role_type:           str           = "Software Engineering"
    application_status: str            = "Unknown"
    skill_tags:         list[str]      = []
    skill_alignment:    str            = ""
    relevance_reason:   str            = ""
    notes:              str            = ""
    apply_link:         Optional[str]  = None


def _next_client() -> tuple[genai.Client, int]:
    global _client_index
    idx    = _client_index % len(_gemini_clients)
    client = _gemini_clients[idx]
    _client_index += 1
    return client, idx


def _parse_gemini_response(raw: str, batch: list[dict]) -> list[dict]:
    """
    Parse Gemini's JSON output into enriched items.

    With response_schema enforced on the API call, `raw` should already be a
    clean JSON array — the markdown-fence stripping below is a defensive
    fallback in case a caller passes fenced text directly (e.g. in tests).
    Each item is validated against GeminiListingResult, so malformed shapes
    raise pydantic.ValidationError instead of silently producing bad data.
    """
    raw = raw.strip()
    if raw.startswith("```"):
        parts = raw.split("```")
        raw   = parts[1] if len(parts) > 1 else raw
        if raw.startswith("json"):
            raw = raw[4:]
    raw = raw.strip()

    parsed_raw: list[dict] = json.loads(raw)   # raises JSONDecodeError on bad output
    results: list[dict] = []
    for raw_item in parsed_raw:
        item = GeminiListingResult.model_validate(raw_item)   # raises ValidationError on bad shape
        if not item.is_valid or not (0 <= item.index < len(batch)):
            continue
        enriched = {
            **batch[item.index],
            "company":            item.company,
            "position":           item.position,
            "pay":                item.pay,
            "location":           item.location,
            "role_mode":          item.role_mode,
            "role_type":          item.role_type,
            "application_status": item.application_status,
            "skill_tags":         item.skill_tags,
            "skill_alignment":    item.skill_alignment,
            "relevance_reason":   item.relevance_reason,
            "notes":              item.notes,
            "apply_link":         item.apply_link,
            "confidence":         item.confidence,
            "source_confidence":  item.source_confidence,
        }
        results.append(enriched)
    return results


# ── Backoff schedule: 60 s → 120 s → 240 s → 300 s (cap), crash if still failing
_BACKOFF_STEPS  = [60, 120, 240, 300]   # seconds
_MAX_BACKOFF_S  = 300                   # 5 min hard cap — crash after this


async def validate_batch(batch: list[dict]) -> list[dict]:
    """
    Call Gemini to validate + enrich a batch of raw listings.

    Retry strategy:
      1. Try every key in the pool once (round-robin) for any transient / quota error.
      2. If ALL keys are exhausted, enter exponential backoff:
           60 s → 120 s → 240 s → 300 s
      3. After each backoff pause, retry the full key pool again.
      4. If the batch still fails after the final backoff step (5 min), raise
         RuntimeError — the pipeline will crash rather than silently drop data.

    JSON parse errors are NOT retried (bad output won't fix itself by waiting).
    """
    numbered = "\n\n".join(
        f"[{i}] Source: {t['source']} | Posted: {t.get('created_at', 'Unknown')}\n"
        f"{t['text']}\nURL: {t['url']}"
        for i, t in enumerate(batch)
    )
    prompt   = GEMINI_PROMPT + numbered
    n_keys   = len(_gemini_clients)

    def _try_all_keys() -> list[dict] | None:
        """
        Attempt every key once. Returns parsed results on first success,
        or None if every key failed with a retriable error.
        Raises JSONDecodeError/ValidationError immediately on bad output (not retriable).
        """
        for _ in range(n_keys):
            client, idx = _next_client()
            key_label   = f"key {idx + 1}/{n_keys}"
            try:
                response = client.models.generate_content(
                    model="gemini-3.1-flash-lite",
                    contents=prompt,
                    config=genai_types.GenerateContentConfig(
                        response_mime_type="application/json",
                        response_schema=list[GeminiListingResult],
                    ),
                )
                results = _parse_gemini_response(response.text, batch)
                print(f"    ✓ Batch validated ({key_label})")
                return results

            except (json.JSONDecodeError, ValidationError) as e:
                # Malformed output — retrying won't help; bail immediately
                print(f"  ⚠  Gemini parse error ({key_label}): {e}")
                raise   # propagate so the outer loop doesn't keep retrying

            except Exception as e:
                err      = str(e).lower()
                is_quota = any(x in err for x in ["429", "quota", "rate", "resource exhausted"])
                tag      = "quota" if is_quota else "error"
                print(f"  ⚠  {key_label} {tag}: {e} — trying next key")
                # Always rotate and try the next key, regardless of error type

        return None   # all keys failed

    # ── Outer retry loop with backoff ─────────────────────────────────────────
    backoff_iter = iter(_BACKOFF_STEPS)

    while True:
        try:
            result = _try_all_keys()
        except (json.JSONDecodeError, ValidationError):
            return []   # unrecoverable — skip this batch silently

        if result is not None:
            return result

        # All keys failed — decide whether to backoff or crash
        try:
            wait = next(backoff_iter)
        except StopIteration:
            # We've exhausted the backoff schedule — this is unrecoverable
            raise RuntimeError(
                f"Gemini: all {n_keys} key(s) failed across all backoff attempts "
                f"(max {_MAX_BACKOFF_S}s). Pipeline aborted."
            )

        print(
            f"  ⏳ All Gemini keys exhausted — backing off {wait}s "
            f"(max {_MAX_BACKOFF_S}s before crash) …"
        )
        await asyncio.sleep(wait)
        print("  🔄 Retrying batch after backoff …")


async def filter_already_scraped(listings: list[dict]) -> list[dict]:
    """
    Drop any listing whose scrapedId already exists in the DB.
    Uses a single $in query against the unique index — fast even at scale.
    """
    col = get_signals()
    candidate_ids = [build_scraped_id(item) for item in listings]

    existing_cursor = col.find(
        {"scrapedId": {"$in": candidate_ids}},
        {"scrapedId": 1, "_id": 0},
    )
    existing_ids: set[str] = {
        doc["scrapedId"] async for doc in existing_cursor
    }

    if existing_ids:
        print(f"  🗑  Dropping {len(existing_ids)} already-scraped listings before Gemini")

    return [item for item in listings if build_scraped_id(item) not in existing_ids]


async def save_batch(col: AsyncIOMotorCollection, items: list[dict]) -> tuple[int, int]:
    """
    Upsert a list of validated+enriched items into the DB immediately.
    Returns (saved, skipped) counts for this batch.
    Uses bulk_write for efficiency — one round-trip per batch.
    """
    from pymongo import UpdateOne
    from pymongo.errors import BulkWriteError

    if not items:
        return 0, 0

    ops = [
        UpdateOne(
            {"scrapedId": doc["scrapedId"]},
            {"$setOnInsert": doc},
            upsert=True,
        )
        for doc in (item_to_doc(item) for item in items)
    ]

    try:
        result = await col.bulk_write(ops, ordered=False)
        saved   = result.upserted_count
        skipped = len(ops) - saved
        return saved, skipped
    except BulkWriteError as bwe:
        # Some succeeded, some were duplicate-key errors — that's fine
        saved   = bwe.details.get("nUpserted", 0)
        skipped = sum(
            1 for e in bwe.details.get("writeErrors", [])
            if e.get("code") == 11000
        )
        other_errors = [
            e for e in bwe.details.get("writeErrors", [])
            if e.get("code") != 11000
        ]
        for e in other_errors:
            print(f"  ⚠  DB write error: {e}")
        return saved, skipped


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# PIPELINE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
async def run_scrape_pipeline() -> dict[str, int]:
    if scrape_state["running"]:
        print("⚠  Scrape already in progress, skipping.")
        return {"saved": 0, "skipped": 0}
 
    # ── reset progress ────────────────────────────────────────────────────
    scrape_state.update({
        "running":         True,
        "progress":        0,
        "total_steps":     0,
        "completed_steps": 0,
        "current_source":  "Starting…",
        "phase":           "fetching",
    })
 
    col   = get_signals()
    saved = skipped = 0
 
    try:
        print(f"\n🕐 [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Scrape started")
        await log_event("Scrape pipeline started", "active")
 
        # ── PHASE 1: fetch (0 → 40%) ──────────────────────────────────────
        scrape_state["phase"]    = "fetching"
        scrape_state["progress"] = 5
 
        web_sources = [
            ("Remotive",  fetch_remotive),
            ("Jobberman", fetch_jobberman),
            ("MyJobMag",  fetch_myjobmag),
            ("Himalayas", fetch_himalayas),
            ("RemoteOK",  fetch_remoteok),
            ("Bluesky",   fetch_bluesky),
        ]
        scrape_state["current_source"] = f"Fetching {len(web_sources)} sources in parallel…"
        web_results_map: dict[str, list[dict]] = {}
        async with httpx.AsyncClient() as client:
            async def _fetch_named(name: str, fn) -> tuple[str, list[dict]]:
                return name, await fn(client)

            tasks = [asyncio.create_task(_fetch_named(name, fn)) for name, fn in web_sources]
            for completed_n, coro in enumerate(asyncio.as_completed(tasks), start=1):
                name, result = await coro
                web_results_map[name] = result
                await record_source_health(name, len(result))
                await log_event(f"Fetched {len(result)} listings from {name}", "info")
                scrape_state["current_source"] = f"{name} done"
                scrape_state["progress"]       = 5 + int((completed_n / len(web_sources)) * 35)

        web_results: list[list[dict]] = [web_results_map[name] for name, _ in web_sources]
        scrape_state["progress"]      = 40
        scrape_state["current_source"] = "Telegram channels"
        tg = await fetch_telegram()  # updates progress 40→70 internally
 
        # ── combine & dedupe ──────────────────────────────────────────────
        scrape_state["progress"] = 70
        combined: list[dict] = []
        seen_ids: set[str]   = set()
        for source_list in [*web_results, tg]:
            for item in source_list:
                uid = f"{item['source']}:{item['id']}"
                if uid not in seen_ids:
                    seen_ids.add(uid)
                    combined.append(item)
 
        await log_event(f"Telegram scan complete — {len(tg)} messages matched", "info")
        print(f"  📦 {len(combined)} total unique raw listings")
 
        if not combined:
            scrape_state.update({"progress": 100, "phase": "done", "current_source": None})
            return {"saved": 0, "skipped": 0}
 
        # ── PHASE 2: dedup check (70 → 75%) ──────────────────────────────
        scrape_state["phase"]          = "validating"
        scrape_state["current_source"] = "Checking duplicates"
        fresh = await filter_already_scraped(combined)
        scrape_state["progress"] = 75
        print(f"  📬 {len(fresh)} new listings (dropped {len(combined) - len(fresh)} dupes)")
        await log_event(
        f"{len(fresh)} new listings after dedup (dropped {len(combined) - len(fresh)})",
        "info",
    )
 
        if not fresh:
            scrape_state.update({
                "progress": 100, "phase": "done",
                "last_run": datetime.now(timezone.utc).isoformat(),
                "last_saved": 0, "current_source": None,
            })
            return {"saved": 0, "skipped": len(combined)}
 
        # ── PHASE 3: Gemini validation (75 → 95%) ────────────────────────
        total_batches = -(-len(fresh) // BATCH_SIZE)
        scrape_state["total_steps"] = total_batches
 
        for batch_num, i in enumerate(range(0, len(fresh), BATCH_SIZE), start=1):
            batch = fresh[i: i + BATCH_SIZE]
            scrape_state["current_source"] = f"AI validation — batch {batch_num}/{total_batches}"
            # 75% + up to 20% spread across batches
            scrape_state["progress"] = 75 + int((batch_num / total_batches) * 20)
            scrape_state["completed_steps"] = batch_num
 
            print(f"  [{batch_num}/{total_batches}] Validating items {i+1}–{i+len(batch)}…")
            validated = await validate_batch(batch)
 
            if not validated:
                skipped += len(batch)
                continue
 
            # ── PHASE 4: save (95 → 99%) ──────────────────────────────────
            scrape_state["phase"] = "saving"
            b_saved, b_skipped = await save_batch(col, validated)
            saved   += b_saved
            skipped += b_skipped
            scrape_state["last_saved"] = saved
            print(f"    ↳ {b_saved} saved, {b_skipped} skipped  (total: {saved})")
            await log_event(
                f"Batch {batch_num}/{total_batches} — {b_saved} saved, {b_skipped} skipped",
                "high" if b_saved > 0 else "info",
            )
 
        scrape_state.update({
            "last_run":        datetime.now(timezone.utc).isoformat(),
            "last_saved":      saved,
            "progress":        100,
            "phase":           "done",
            "current_source":  None,
        })
        print(f"✅ Pipeline done — saved {saved}, skipped {skipped}\n")
        await log_event(
            f"Pipeline complete — {saved} new signals saved, {skipped} skipped",
            "high" if saved > 0 else "info",
        )
        return {"saved": saved, "skipped": skipped}
    except Exception as e:
        print(f"❌ Pipeline error: {e}")
        await log_event(f"Pipeline error: {str(e)}", "error")

    finally:
        scrape_state["running"] = False

async def generate_notifications_pipeline() -> int:
    """
    Analyse the last 7 days of scraped IntelligenceSignal documents and
    derive NotificationItem records for signal.tsx.
 
    Runs once a week via APScheduler, or on-demand via POST /notifications/generate.
    Fully replaces the notifications collection on every run — no stale data.
 
    Notification derivation rules
    ─────────────────────────────────────────────────────────────────────────────
    section         source                         category
    ──────────────  ─────────────────────────────  ────────────────────────────
    Today           addedAt = today                High Match Opportunity  (score ≥ 85)
    Today           addedAt = today                Deadline Alert          (applicationStatus = "Closing soon")
    Today           addedAt = today                Trending Opportunity    (score 65–84)
    Earlier…        addedAt 2–6 days ago           Queue Reminder          (status="new", isIntearested=True)
    Intelligence…   aggregate analysis             AI Insight Alert        (Gemini-generated)
    Opportunity…    batch from whole week          High Match Opportunity  (≥3 signals with score ≥ 80)
    System          last scrape stats              System Activity         (always generated)
    ─────────────────────────────────────────────────────────────────────────────
    aiMatchScore mapping (from EXTRACTION_CONFIDENCE_SCORE in main.py):
      "High"   → 90  →  score ≥ 85  →  High Match Opportunity / high priority
      "Medium" → 65  →  score 65–84 →  Trending Opportunity   / medium priority
      "Low"    → 40  →  score < 65  →  (omitted from Today notifications)
    """
    if notifications_state["running"]:
        print("⚠  Notification generation already in progress, skipping.")
        return 0
 
    notifications_state["running"] = True
    notif_col = get_notifications_col()
    sig_col   = get_signals()
    now       = datetime.now(timezone.utc)
 
    today_start  = now.replace(hour=0,  minute=0,  second=0,  microsecond=0)
    two_days_ago = now - timedelta(days=2)
    six_days_ago = now - timedelta(days=6)
    week_ago     = now - timedelta(days=7)
 
    try:
        print(f"\n🔔 [{now.strftime('%Y-%m-%d %H:%M:%S')}] Notification generation started")
 
        # ── Fetch all signals from the past 7 days ───────────────────────────
        recent: list[dict] = await sig_col.find(
            {"addedAt": {"$gte": week_ago.isoformat()}}
        ).sort([("aiMatchScore", -1), ("_id", -1)]).to_list(length=600)
 
        print(f"  📊 {len(recent)} signals in the past 7 days")
 
        notifications: list[dict] = []
        gen_time = now.isoformat()
 
        # ════════════════════════════════════════════════════════════════════
        # 1.  TODAY  —  High Match Opportunity  (score ≥ 85)
        # ════════════════════════════════════════════════════════════════════
        today_signals = [
            s for s in recent
            if s.get("addedAt", "") >= today_start.isoformat()
        ]
 
        high_match_today = [
            s for s in today_signals
            if s.get("aiMatchScore", 0) >= 85
        ][:4]   # cap at 4 cards to avoid notification flooding
 
        for sig in high_match_today:
            role        = sig.get("role",    "tech role")
            company     = sig.get("company", "a company")
            score       = sig.get("aiMatchScore", 90)
            platform    = sig.get("platform", "Signal")
            skill_align = sig.get("skillAlignment", "")
            reason      = sig.get("relevanceReason", "")
            summary     = sig.get("aiSummary", "")
 
            # Context: prefer relevanceReason (most specific), then aiSummary
            context_text = _clean(reason or summary or f"{role} at {company} on {platform}.", 200)
 
            notifications.append({
                "_id":         _notif_id("hm", str(sig["_id"])),
                "id":          _notif_id("hm", str(sig["_id"])),
                "section":     "Today",
                "title":       f"New {role} detected — {score}% match",
                "context":     context_text,
                "time":        _fmt_time(sig.get("addedAt", ""), now),
                "category":    "High Match Opportunity",
                "priority":    "high",
                "matchScore":  score,
                "urgency":     None,
                "actions":     ["Save", "Open", "Mark important", "Mute similar"],
                "signalId":    str(sig["_id"]),
                "generatedAt": gen_time,
                "isRead":      False,
            })
 
        # ════════════════════════════════════════════════════════════════════
        # 2.  TODAY  —  Deadline Alert  (applicationStatus = "Closing soon")
        # ════════════════════════════════════════════════════════════════════
        deadline_signals = [
            s for s in recent
            if s.get("applicationStatus") == "Closing soon"
        ][:3]
 
        for sig in deadline_signals:
            role    = sig.get("role",    "this role")
            company = sig.get("company", "")
            added   = sig.get("addedAt", "")
            summary = _clean(sig.get("aiSummary", ""), 120)
 
            # Compute urgency label from addedAt (best proxy we have for deadline proximity)
            try:
                dt       = datetime.fromisoformat(added)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                days_old = (now - dt).days
                if days_old == 0:
                    urgency_label = "24h"
                    urgency_text  = "closes today"
                elif days_old <= 2:
                    urgency_label = f"{days_old}d"
                    urgency_text  = f"closing in {days_old} day{'s' if days_old != 1 else ''}"
                else:
                    urgency_label = "soon"
                    urgency_text  = "closing soon"
            except Exception:
                urgency_label = "soon"
                urgency_text  = "closing soon"
 
            co_str  = f" at {company}" if company and company != "Unknown" else ""
            context = f"{role}{co_str} is {urgency_text}. {summary}".strip()
 
            notifications.append({
                "_id":         _notif_id("dl", str(sig["_id"])),
                "id":          _notif_id("dl", str(sig["_id"])),
                "section":     "Today",
                "title":       f"Application deadline approaching — {role}{co_str}",
                "context":     _clean(context, 200),
                "time":        _fmt_time(added, now),
                "category":    "Deadline Alert",
                "priority":    "high",
                "matchScore":  None,
                "urgency":     urgency_label,
                "actions":     ["Open", "Save", "Dismiss"],
                "signalId":    str(sig["_id"]),
                "generatedAt": gen_time,
                "isRead":      False,
            })
 
        # ════════════════════════════════════════════════════════════════════
        # 3.  TODAY  —  Trending Opportunity  (score 65–84, most skillTags)
        # ════════════════════════════════════════════════════════════════════
        trending_today = sorted(
            [
                s for s in today_signals
                if 65 <= s.get("aiMatchScore", 0) < 85
            ],
            key=lambda s: len(s.get("skillTags", [])),
            reverse=True,
        )[:2]
 
        for sig in trending_today:
            role     = sig.get("role",     "tech role")
            tags     = sig.get("skillTags", [])[:3]
            platform = sig.get("platform", "Signal")
            tag_str  = ", ".join(tags) if tags else "your primary stack"
 
            notifications.append({
                "_id":         _notif_id("tr", str(sig["_id"])),
                "id":          _notif_id("tr", str(sig["_id"])),
                "section":     "Today",
                "title":       f"This {role} is gaining attention quickly",
                "context":     (
                    f"View velocity is rising among candidates with {tag_str} "
                    f"in their stack. Role sourced from {platform}."
                ),
                "time":        _fmt_time(sig.get("addedAt", ""), now),
                "category":    "Trending Opportunity",
                "priority":    "medium",
                "matchScore":  None,
                "urgency":     None,
                "actions":     ["Open", "Mark important", "Mute similar"],
                "signalId":    str(sig["_id"]),
                "generatedAt": gen_time,
                "isRead":      False,
            })
 
        # ════════════════════════════════════════════════════════════════════
        # 4.  EARLIER THIS WEEK  —  Queue Reminder
        #     Signals the user hasn't acted on in 2–6 days
        # ════════════════════════════════════════════════════════════════════
        queue_signals = sorted(
            [
                s for s in recent
                if (
                    s.get("isIntearested", True)
                    and s.get("status") == "new"
                    and six_days_ago.isoformat() <= s.get("addedAt", "") < two_days_ago.isoformat()
                )
            ],
            key=lambda s: s.get("aiMatchScore", 0),
            reverse=True,
        )[:3]
 
        for sig in queue_signals:
            role    = sig.get("role",    "this role")
            company = sig.get("company", "")
            added   = sig.get("addedAt", "")
            mode    = sig.get("roleMode", "Remote")
            loc     = sig.get("location", "")
            summary = _clean(sig.get("aiSummary", ""), 120)
 
            try:
                dt       = datetime.fromisoformat(added)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                days_ago  = (now - dt).days
                days_str  = f"{days_ago} day{'s' if days_ago != 1 else ''} ago"
            except Exception:
                days_str = "recently"
 
            co_str  = f" at {company}" if company and company != "Unknown" else ""
            loc_str = f", {loc}" if loc and loc not in ("Unknown", "") else ""
            context = f"{role}{co_str} ({mode}{loc_str}) has not been submitted yet. {summary}".strip()
 
            notifications.append({
                "_id":         _notif_id("qr", str(sig["_id"])),
                "id":          _notif_id("qr", str(sig["_id"])),
                "section":     "Earlier This Week",
                "title":       f"You marked this role as interested {days_str}",
                "context":     _clean(context, 200),
                "time":        _fmt_time(added, now),
                "category":    "Queue Reminder",
                "priority":    "medium",
                "matchScore":  None,
                "urgency":     None,
                "actions":     ["Open", "Dismiss"],
                "signalId":    str(sig["_id"]),
                "generatedAt": gen_time,
                "isRead":      False,
            })
 
        # ════════════════════════════════════════════════════════════════════
        # 5.  INTELLIGENCE UPDATES  —  AI Insight Alert  (Gemini-powered)
        # ════════════════════════════════════════════════════════════════════
        if recent:
            insight = await _generate_ai_insight(recent, now)
            if insight.get("title"):
                notifications.append({
                    "_id":         _notif_id("ai", gen_time[:10]),   # one per day
                    "id":          _notif_id("ai", gen_time[:10]),
                    "section":     "Intelligence Updates",
                    "title":       insight["title"],
                    "context":     insight["context"],
                    "time":        _fmt_time(gen_time, now),
                    "category":    "AI Insight Alert",
                    "priority":    "high",
                    "matchScore":  None,
                    "urgency":     None,
                    "actions":     ["Open", "Save"],
                    "signalId":    None,
                    "generatedAt": gen_time,
                    "isRead":      False,
                })
 
        # ════════════════════════════════════════════════════════════════════
        # 6.  OPPORTUNITY ALERTS  —  Batch high-match summary
        #     Triggers when ≥ 3 new unread signals scored ≥ 80 this week
        # ════════════════════════════════════════════════════════════════════
        batch_signals = [
            s for s in recent
            if s.get("aiMatchScore", 0) >= 80 and s.get("status") == "new"
        ]
 
        if len(batch_signals) >= 3:
            top_score   = max(s.get("aiMatchScore", 0) for s in batch_signals)
            # Collect the most common role mode
            mode_tally: dict[str, int] = {}
            for s in batch_signals:
                m = s.get("roleMode", "Remote")
                mode_tally[m] = mode_tally.get(m, 0) + 1
            dominant_mode = max(mode_tally, key=mode_tally.get, default="Remote")   # type: ignore[arg-type]
 
            # Deduplicated common skill tags across top 8 signals
            seen_tags: set[str] = set()
            common_tags: list[str] = []
            for s in batch_signals[:8]:
                for tag in s.get("skillTags", []):
                    if tag and tag not in seen_tags:
                        seen_tags.add(tag)
                        common_tags.append(tag)
                    if len(common_tags) >= 5:
                        break
                if len(common_tags) >= 5:
                    break
 
            tag_str = ", ".join(common_tags[:4]) if common_tags else "your stack"
 
            notifications.append({
                "_id":         _notif_id("oa", gen_time[:10]),
                "id":          _notif_id("oa", gen_time[:10]),
                "section":     "Opportunity Alerts",
                "title":       (
                    f"{len(batch_signals)} new {dominant_mode.lower()} "
                    f"internship{'s' if len(batch_signals) != 1 else ''} match your profile"
                ),
                "context":     (
                    f"All {len(batch_signals)} roles mention {tag_str} in their scope — "
                    f"strong alignment with your current stack."
                ),
                "time":        _fmt_time(gen_time, now),
                "category":    "High Match Opportunity",
                "priority":    "high",
                "matchScore":  top_score,
                "urgency":     None,
                "actions":     ["Open", "Save", "Mute similar"],
                "signalId":    None,
                "generatedAt": gen_time,
                "isRead":      False,
            })
 
        # ════════════════════════════════════════════════════════════════════
        # 7.  SYSTEM ACTIVITY  —  Scraper stats
        # ════════════════════════════════════════════════════════════════════
        total_week     = len(recent)
        high_sig_count = len([s for s in recent if s.get("aiMatchScore", 0) >= 80])
        platforms_seen = sorted({s.get("platform", "Unknown") for s in recent} - {"Unknown"})[:4]
        platform_str   = ", ".join(platforms_seen) if platforms_seen else "multiple sources"
        last_run_ts    = scrape_state.get("last_run")
        last_run_str   = _fmt_time(last_run_ts, now) if last_run_ts else "recently"
 
        notifications.append({
            "_id":         _notif_id("sys", gen_time[:10]),
            "id":          _notif_id("sys", gen_time[:10]),
            "section":     "System Activity",
            "title":       f"Scanner ingested {total_week} new opportunities this week",
            "context":     (
                f"Sources: {platform_str}. "
                f"{high_sig_count} filtered as potential high-signal matches for your profile. "
                f"Last scan: {last_run_str}."
            ),
            "time":        _fmt_time(gen_time, now),
            "category":    "System Activity",
            "priority":    "low",
            "matchScore":  None,
            "urgency":     None,
            "actions":     ["Dismiss"],
            "signalId":    None,
            "generatedAt": gen_time,
            "isRead":      False,
        })
 
        # ── Persist: drop stale, insert fresh ────────────────────────────────
        important_cursor = notif_col.find(
            {"isImportant": True},
            {"id": 1, "_id": 0},
        )
        important_ids: set[str] = {
            doc["id"] async for doc in important_cursor
        }
 
        # 2. Wipe old notifications
        await notif_col.delete_many({})
 
        # 3. Re-apply isImportant to any notification whose ID survived regen
        if important_ids:
            for notif in notifications:
                if notif["id"] in important_ids:
                    notif["isImportant"] = True
 
        # 4. Insert fresh batch
        if notifications:
            await notif_col.insert_many(notifications)

        if notifications:
            await notif_col.insert_many(notifications)
 
        notifications_state["last_run"]   = gen_time
        notifications_state["last_count"] = len(notifications)
 
        print(f"✅ Notifications generated — {len(notifications)} items written\n")
        return len(notifications)
 
    finally:
        notifications_state["running"] = False
 

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CLEANUP
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
_CLEANUP_PROTECTED_STATUSES = {"saved", "applied", "interviewing", "offered", "rejected"}


async def cleanup_old_signals(days: int = 30) -> dict[str, int]:
    """Delete unactioned signals and old notifications older than `days` days."""
    cutoff    = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    sig_col   = get_signals()
    notif_col = get_notifications_col()

    sig_result = await sig_col.delete_many({
        "addedAt": {"$lt": cutoff},
        "status":  {"$nin": list(_CLEANUP_PROTECTED_STATUSES)},
    })
    notif_result = await notif_col.delete_many({"generatedAt": {"$lt": cutoff}})

    print(
        f"🧹 Cleanup: {sig_result.deleted_count} old signals, "
        f"{notif_result.deleted_count} old notifications removed (cutoff: {cutoff[:10]})"
    )
    return {
        "signals_deleted":       sig_result.deleted_count,
        "notifications_deleted": notif_result.deleted_count,
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# APP + SCHEDULER
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
scheduler = AsyncIOScheduler(timezone="Africa/Lagos")


@asynccontextmanager
async def lifespan(app: FastAPI):
    await connect_db()
    scheduler.add_job(
        run_scrape_pipeline,
        trigger="cron",
        hour=SCRAPE_HOUR,
        minute=SCRAPE_MINUTE,
        id="daily_scrape",
        replace_existing=True,
    )
    scheduler.add_job(
        generate_notifications_pipeline,
        trigger="cron",
        day_of_week="sun",   # every Sunday — one day after Saturday's big scrape window
        hour=6,
        minute=0,
        id="weekly_notifications",
        replace_existing=True,
    )
    scheduler.add_job(
        cleanup_old_signals,
        trigger="cron",
        day_of_week="mon",   # Monday at 3 AM — after Sunday notifications, before morning usage
        hour=3,
        minute=0,
        id="weekly_cleanup",
        replace_existing=True,
    )
    scheduler.start()
    notif_next = scheduler.get_job("weekly_notifications").next_run_time
    print(f"🔔 Notification cron — next run: {notif_next.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    cleanup_next = scheduler.get_job("weekly_cleanup").next_run_time
    print(f"🧹 Cleanup cron — next run: {cleanup_next.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    
    next_run = scheduler.get_job("daily_scrape").next_run_time
    print(f"⏰ Cron scheduled — next run: {next_run.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    yield
    scheduler.shutdown(wait=False)
    await close_db()


app = FastAPI(
    title="Jobless API",
    description="Scrapes job signals and manages them via a pipeline.",
    version="2.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


def valid_object_id(id: str) -> ObjectId:
    try:
        return ObjectId(id)
    except Exception:
        raise HTTPException(status_code=400, detail=f"Invalid signal ID: {id}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ROUTES
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

app.include_router(health_router)   

@app.get("/signals", response_model=PaginatedSignals)
async def get_signals_route(
    page:                 int                         = Query(default=1,   ge=1),
    limit:                int                         = Query(default=100, ge=1, le=200),
    status:               Optional[PipelineStage]     = Query(default=None),
    platform:             Optional[str]               = Query(default=None),
    location:             Optional[str]               = Query(default=None),
    isSaved:              Optional[bool]              = Query(default=None),
    role_mode:            Optional[RoleMode]           = Query(default=None),
    role_type:            Optional[str]               = Query(default=None),
    application_status:   Optional[ApplicationStatus] = Query(default=None),
    extraction_confidence:Optional[ConfidenceLevel]   = Query(default=None),
    source_confidence:    Optional[ConfidenceLevel]   = Query(default=None),
    skill_tag:            Optional[str]               = Query(default=None),
    # ── new params ───────────────────────────────────────────────────────
    search:               Optional[str]               = Query(default=None, description="Full-text search across role, company, summary, skills"),
    role_category:        Optional[str]               = Query(default=None, description="frontend|backend|ai|data|mobile|devops|qa|design"),
    location_filter:      Optional[str]               = Query(default=None, description="remote|nigeria|global"),
    skill_filter:         Optional[str]               = Query(default=None, description="react|python|etc — skill keyword"),
    sort:                 str                         = Query(default="newest"),
):
    col   = get_signals()
    skip  = (page - 1) * limit
    query: dict[str, Any] = {}

    if status:                query["status"]               = status.value
    if platform:              query["platform"]             = platform
    if isSaved is not None:   query["isSaved"]              = isSaved
    if role_mode:             query["roleMode"]             = role_mode.value
    if role_type:             query["roleType"]             = role_type
    if application_status:    query["applicationStatus"]    = application_status.value
    if extraction_confidence: query["extractionConfidence"] = extraction_confidence.value
    if source_confidence:     query["sourceConfidence"]     = source_confidence.value

    # ── role_category → roleType regex ───────────────────────────────────
    ROLE_CATEGORY_MAP: dict[str, Any] = {
        "frontend": {"roleType": {"$regex": "Software Engineering", "$options": "i"},
                     "$or": [{"role": {"$regex": "front", "$options": "i"}},
                              {"skillTags": {"$elemMatch": {"$regex": "react|vue|angular|css|html|svelte|next", "$options": "i"}}}]},
        "backend":  {"$or": [{"role": {"$regex": "back|server|api|node|django|flask|fastapi|spring|rails|laravel", "$options": "i"}},
                              {"skillTags": {"$elemMatch": {"$regex": "node|python|java|go|rust|php|ruby|postgres|mysql|mongo|redis", "$options": "i"}}}]},
        "ai":       {"$or": [{"role": {"$regex": "ai|ml|machine learning|data scien|nlp|llm|deep learn", "$options": "i"}},
                              {"roleType": {"$regex": "Data", "$options": "i"}},
                              {"skillTags": {"$elemMatch": {"$regex": "tensorflow|pytorch|sklearn|hugging|llm|openai|langchain", "$options": "i"}}}]},
        "data":     {"$or": [{"roleType": {"$regex": "Data", "$options": "i"}},
                              {"role": {"$regex": "data|analyst|analytics|bi |tableau|dbt|spark|airflow", "$options": "i"}}]},
        "mobile":   {"$or": [{"roleType": {"$regex": "Mobile", "$options": "i"}},
                              {"role": {"$regex": "mobile|android|ios|flutter|react native|kotlin|swift", "$options": "i"}}]},
        "devops":   {"$or": [{"roleType": {"$regex": "DevOps", "$options": "i"}},
                              {"role": {"$regex": "devops|sre|cloud|infra|kubernetes|docker|cicd|platform engineer", "$options": "i"}}]},
        "qa":       {"$or": [{"roleType": {"$regex": "QA", "$options": "i"}},
                              {"role": {"$regex": "qa|quality|test|sdet|automation engineer", "$options": "i"}}]},
        "design":   {"$or": [{"roleType": {"$regex": "Design", "$options": "i"}},
                              {"role": {"$regex": "design|ux|ui |figma|product design", "$options": "i"}}]},
    }

    if role_category and role_category.lower() in ROLE_CATEGORY_MAP:
        cat_query = ROLE_CATEGORY_MAP[role_category.lower()]
        # Merge $or carefully — if cat_query IS an $or, wrap with $and
        if "$or" in cat_query and "$or" in query:
            query = {"$and": [query, cat_query]}
        else:
            query.update(cat_query)

    # ── location_filter ───────────────────────────────────────────────────
    if location_filter:
        lf = location_filter.lower()
        if lf == "remote":
            query["roleMode"] = "Remote"
        elif lf == "nigeria":
            query["location"] = {"$regex": "nigeria", "$options": "i"}
        elif lf == "global":
            query["$and"] = query.get("$and", []) + [
                {"location": {"$not": {"$regex": "nigeria", "$options": "i"}}},
                {"roleMode": "Remote"},
            ]
    elif location:
        query["location"] = location

    # ── skill_filter (substring in skillTags array) ───────────────────────
    if skill_filter:
        query["skillTags"] = {"$elemMatch": {"$regex": skill_filter, "$options": "i"}}
    elif skill_tag:
        query["skillTags"] = {"$elemMatch": {"$regex": skill_tag, "$options": "i"}}

    # ── full-text search ──────────────────────────────────────────────────
    if search and search.strip():
        s = search.strip()
        search_or = [
            {"role":      {"$regex": s, "$options": "i"}},
            {"company":   {"$regex": s, "$options": "i"}},
            {"aiSummary": {"$regex": s, "$options": "i"}},
            {"skillTags": {"$elemMatch": {"$regex": s, "$options": "i"}}},
            {"skillAlignment": {"$regex": s, "$options": "i"}},
        ]
        if "$and" in query:
            query["$and"].append({"$or": search_or})
        elif "$or" in query:
            query = {"$and": [query, {"$or": search_or}]}
        else:
            query["$or"] = search_or

    SORT_FIELDS = {
        "newest":   [("postedAt", -1), ("addedAt",  -1)],
        "oldest":   [("postedAt",  1), ("addedAt",   1)],
        "match":    [("aiMatchScore", -1), ("postedAt", -1)],
        "platform": [("platform", 1), ("addedAt", -1)],
    }
    sort_fields = SORT_FIELDS.get(sort, SORT_FIELDS["newest"])

    docs, total = await asyncio.gather(
        col.find(query).sort(sort_fields).skip(skip).limit(limit).to_list(length=limit),
        col.count_documents(query),
    )

    return PaginatedSignals(
        signals=[doc_to_signal(d) for d in docs],
        total=total,
        page=page,
        pages=-(-total // limit),
        limit=limit,
    )

# ── GET /signals/{id} ─────────────────────────────────────────────────────────
@app.get("/signals/{id}", response_model=IntelligenceSignal)
async def get_signal(id: str):
    col = get_signals()
    doc = await col.find_one({"_id": valid_object_id(id)})
    if not doc:
        raise HTTPException(status_code=404, detail="Signal not found.")
    return doc_to_signal(doc)


# ── RELATEDNESS (content-based, computed on read) ───────────────────────────────
# Two signals are "related" when they point at the same kind of opportunity. We
# score it from data we already extract — no embeddings, no new storage — so the
# result is always fresh and never references signals the 30-day cleanup removed.
RELATED_POOL_LIMIT = 250   # most-recent candidates to score per request
RELATED_MIN_SCORE  = 12    # below this, treat as unrelated (drop)


def _norm_tags(tags: Any) -> set[str]:
    return {t.strip().lower() for t in (tags or []) if t and t.strip()}


def _seniority_tier(doc: dict) -> str:
    """'junior' | 'senior' | 'unknown' from keywords + Gemini's confidence vote."""
    text = " ".join([
        doc.get("role") or "", doc.get("aiSummary") or "",
        doc.get("originalSourceText") or "",
    ]).lower()
    has_junior = any(kw in text for kw in SENIORITY_KEYWORDS)
    has_senior = any(kw in text for kw in SENIOR_KEYWORDS)
    if (doc.get("extractionConfidence") or "").strip().lower() == "high":
        has_junior = True
    if has_junior and not has_senior:
        return "junior"
    if has_senior and not has_junior:
        return "senior"
    return "unknown"


def relatedness(source: dict, cand: dict) -> float:
    """0-100 similarity between two signal docs. Higher = more related."""
    score = 0.0

    a, b = _norm_tags(source.get("skillTags")), _norm_tags(cand.get("skillTags"))
    if a and b:
        shared = a & b
        jaccard = len(shared) / len(a | b)
        score += jaccard * 50                       # tag overlap is the main driver
        score += min(len(shared), 4) * 5            # reward absolute overlap (≤ +20)

    comp = (source.get("company") or "").strip().lower()
    if comp and comp not in ("unknown", "") and comp == (cand.get("company") or "").strip().lower():
        score += 25                                 # other openings at the same company

    if (source.get("roleType") or "").strip() == (cand.get("roleType") or "").strip():
        score += 15

    src_tier, cand_tier = _seniority_tier(source), _seniority_tier(cand)
    if src_tier != "unknown" and cand_tier != "unknown":
        score += 10 if src_tier == cand_tier else -15   # keep interns with interns,
                                                        # push senior↔junior matches down

    if (source.get("roleMode") or "").strip() == (cand.get("roleMode") or "").strip():
        score += 5

    return score


# ── GET /signals/{id}/related ───────────────────────────────────────────────────
@app.get("/signals/{id}/related", response_model=list[IntelligenceSignal])
async def get_related_signals(id: str, limit: int = Query(default=6, ge=1, le=20)):
    col = get_signals()
    oid = valid_object_id(id)
    source = await col.find_one({"_id": oid})
    if not source:
        raise HTTPException(status_code=404, detail="Signal not found.")

    ranked: list[tuple[float, dict]] = []
    seen: set = {oid}

    # 1. Manually curated links (via PATCH) always come first, if still present.
    curated_ids = [valid_object_id(r) for r in source.get("relatedIds", []) if r]
    if curated_ids:
        async for doc in col.find({"_id": {"$in": curated_ids}}):
            ranked.append((float("inf"), doc))
            seen.add(doc["_id"])

    # 2. Computed matches: prefilter to a recent pool that shares SOMETHING, then
    #    score and keep the strongest. roleType is indexed; the $or keeps it cheap.
    tags = list(_norm_tags(source.get("skillTags")))
    prefilter: dict = {"_id": {"$ne": oid}, "$or": [
        {"roleType": source.get("roleType")},
        {"company":  source.get("company")},
    ]}
    if tags:
        prefilter["$or"].append({"skillTags": {"$in": source.get("skillTags", [])}})

    computed: list[tuple[float, dict]] = []
    cursor = col.find(prefilter).sort("addedAt", -1).limit(RELATED_POOL_LIMIT)
    async for cand in cursor:
        if cand["_id"] in seen:
            continue
        s = relatedness(source, cand)
        if s >= RELATED_MIN_SCORE:
            computed.append((s, cand))

    computed.sort(key=lambda x: x[0], reverse=True)
    ranked.extend(computed)

    return [doc_to_signal(d) for _, d in ranked[:limit]]

# ── PATCH /signals/{id} ───────────────────────────────────────────────────────
@app.patch("/signals/{signal_id}", response_model=IntelligenceSignal)
async def update_signal_status(signal_id: str, body: StatusUpdate):
    col = get_signals()
    oid = valid_object_id(signal_id)

    status = body.status.value
    is_saved = status != "new"

    updated = await col.find_one_and_update(
        {"_id": oid},
        {
            "$set": {
                "status": status,
                "isSaved": is_saved
            }
        },
        return_document=ReturnDocument.AFTER,
    )

    if not updated:
        raise HTTPException(status_code=404, detail="Signal not found.")

    return doc_to_signal(updated)

# ── DELETE /signals/cleanup ───────────────────────────────────────────────────
@app.delete("/signals/cleanup")
async def manual_cleanup(days: int = Query(default=30, ge=1, le=365)):
    """Delete unactioned signals and notifications older than `days` days."""
    result = await cleanup_old_signals(days=days)
    return {
        "message":              "Cleanup complete.",
        "signalsDeleted":       result["signals_deleted"],
        "notificationsDeleted": result["notifications_deleted"],
        "olderThanDays":        days,
    }


# ── DELETE /signals/{id} ──────────────────────────────────────────────────────
@app.delete("/signals/{id}")
async def delete_signal(id: str):
    col     = get_signals()
    oid     = valid_object_id(id)
    deleted = await col.find_one_and_delete({"_id": oid})
    if not deleted:
        raise HTTPException(status_code=404, detail="Signal not found.")
    return {"success": True, "id": id}


# ── PATCH /signals/{id}/related ───────────────────────────────────────────────
@app.patch("/signals/{id}/related")
async def add_related(id: str, related_id: str = Query(...)):
    """Push a related signal ID onto a signal's relatedIds array."""
    col = get_signals()
    oid = valid_object_id(id)
    _   = valid_object_id(related_id)   # validate it too
    await col.update_one({"_id": oid}, {"$addToSet": {"relatedIds": related_id}})
    return {"success": True}


# ── POST /scrape/run ──────────────────────────────────────────────────────────
@app.post("/scrape/run")
async def manual_scrape():
    result = await run_scrape_pipeline()
    return {
        "message": "Scrape complete.",
        "saved":   result["saved"],
        "skipped": result["skipped"],
    }


# ── GET /scrape/status ────────────────────────────────────────────────────────
@app.get("/scrape/status", response_model=ScrapeStatus)
async def scrape_status():
    job      = scheduler.get_job("daily_scrape")
    next_run = job.next_run_time.isoformat() if job and job.next_run_time else None
    return ScrapeStatus(
        running       = scrape_state["running"],
        lastRun       = scrape_state["last_run"],
        lastSaved     = scrape_state["last_saved"],
        nextRun       = next_run,
        progress      = scrape_state["progress"],
        phase         = scrape_state["phase"],
        currentSource = scrape_state["current_source"],
        message       = (
            f"{scrape_state['phase'].capitalize()} — "
            f"{scrape_state['progress']}%"
            + (f" ({scrape_state['current_source']})" if scrape_state["current_source"] else "")
            if scrape_state["running"]
            else f"Next scheduled run: {next_run or 'unknown'}"
        ),
    )


@app.get("/scrape/stream")
async def scrape_stream():
    async def event_generator():
        idle_ticks = 0
        while True:
            payload = {
                "running":       scrape_state["running"],
                "progress":      scrape_state["progress"],
                "phase":         scrape_state["phase"],
                "currentSource": scrape_state["current_source"],
                "lastSaved":     scrape_state["last_saved"],
                "lastRun":       scrape_state["last_run"],
            }
            yield f"data: {json.dumps(payload)}\\n\\n"
 
            if not scrape_state["running"]:
                idle_ticks += 1
                if idle_ticks >= 3:   # 3 s of idle → close stream
                    break
            else:
                idle_ticks = 0
 
            await asyncio.sleep(1)
 
    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",   # disable nginx buffering
        },
    )

@app.get("/channels", response_model=list[TelegramChannel])
async def list_channels():
    col  = get_channels_col()
    docs = await col.find({}).sort("addedAt", 1).to_list(length=500)
    return [TelegramChannel(**{**d, "id": str(d.get("id") or d["_id"])}) for d in docs]

@app.post("/channels", response_model=TelegramChannel, status_code=201)
async def add_channel(body: TelegramChannelCreate):
    col    = get_channels_col()
    handle = body.handle.lstrip("@").strip().lower()
    if not handle:
        raise HTTPException(status_code=400, detail="handle is required")
 
    existing = await col.find_one({"handle": handle})
    if existing:
        raise HTTPException(status_code=409, detail=f"@{handle} already exists")
 
    doc = {
        "_id":     handle,
        "id":      handle,
        "handle":  handle,
        "name":    body.name or f"@{handle}",
        "active":  True,
        "addedAt": datetime.now(timezone.utc).isoformat(),
    }
    await col.insert_one(doc)
    return TelegramChannel(**doc)

@app.patch("/channels/{handle}", response_model=TelegramChannel)
async def patch_channel(handle: str, body: TelegramChannelPatch):
    col    = get_channels_col()
    handle = handle.lstrip("@").lower()
    update: dict = {}
    if body.active  is not None: update["active"] = body.active
    if body.name    is not None: update["name"]   = body.name
    if not update:
        raise HTTPException(status_code=400, detail="Nothing to update")
 
    doc = await col.find_one_and_update(
        {"handle": handle},
        {"$set": update},
        return_document=ReturnDocument.AFTER,
    )
    if not doc:
        raise HTTPException(status_code=404, detail=f"@{handle} not found")
    return TelegramChannel(**{**doc, "id": str(doc.get("id") or doc["_id"])})

@app.delete("/channels/{handle}")
async def delete_channel(handle: str):
    col    = get_channels_col()
    handle = handle.lstrip("@").lower()
    result = await col.delete_one({"handle": handle})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail=f"@{handle} not found")
    return {"success": True, "handle": handle}

@app.get("/monitor", response_model=MonitorResponse)
async def get_monitor():
    sig_col    = get_signals()
    events_col = get_events_col()
    job        = scheduler.get_job("daily_scrape")
    next_run   = job.next_run_time.isoformat() if job and job.next_run_time else None
 
    # ── Run DB queries concurrently ───────────────────────────────────────
    (
        total,
        new_count,
        saved_count,
        high_match,
        platform_pipeline,
        raw_events,
    ) = await asyncio.gather(
        sig_col.count_documents({}),
        sig_col.count_documents({"status": "new"}),
        sig_col.count_documents({"isSaved": True}),
        sig_col.count_documents({"aiMatchScore": {"$gte": 80}}),
        sig_col.aggregate([
            {"$group": {"_id": "$platform", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
        ]).to_list(length=20),
        events_col.find({}).sort("at", -1).limit(15).to_list(length=15),
    )
 
    platform_counts = {doc["_id"]: doc["count"] for doc in platform_pipeline if doc["_id"]}
 
    events_out = [
        PipelineEvent(
            id      = str(e["_id"]),
            message = e["message"],
            status  = e["status"],
            at      = e["at"],
        )
        for e in reversed(raw_events)   # chronological order for the feed
    ]
 
    return MonitorResponse(
        scrapeRunning    = scrape_state["running"],
        scrapePhase      = scrape_state["phase"],
        scrapeProgress   = scrape_state["progress"],
        currentSource    = scrape_state["current_source"],
        lastRun          = scrape_state["last_run"],
        lastSaved        = scrape_state["last_saved"],
        nextRun          = next_run,
 
        totalSignals     = total,
        newSignals       = new_count,
        savedSignals     = saved_count,
        highMatchSignals = high_match,
        platformCounts   = platform_counts,
 
        notifRunning     = notifications_state["running"],
        notifLastRun     = notifications_state["last_run"],
        notifLastCount   = notifications_state["last_count"],
 
        recentEvents     = events_out,
    )

# ── GET /meta ─────────────────────────────────────────────────────────────────
@app.get("/meta")
async def meta():
    """Returns distinct values for every filterable enum field — handy for building filter UIs."""
    col = get_signals()
    fields = [
        "platform", "location", "roleMode", "roleType",
        "applicationStatus", "extractionConfidence", "sourceConfidence", "status",
    ]
    results: dict[str, list] = {}
    for f in fields:
        results[f] = await col.distinct(f)
    # Also return top-20 most common skill tags
    pipeline = [
        {"$unwind": "$skillTags"},
        {"$group": {"_id": "$skillTags", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 20},
    ]
    tag_docs        = await col.aggregate(pipeline).to_list(length=20)
    results["topSkillTags"] = [d["_id"] for d in tag_docs]
    return results

@app.get("/notifications", response_model=list[NotificationItemModel])
async def get_notifications_route(
    section:           Optional[str]  = Query(default=None),
    priority:          Optional[str]  = Query(default=None),
    category:          Optional[str]  = Query(default=None),
    include_dismissed: bool           = Query(default=False,
        description="Pass true to include dismissed notifications (needed for restore)"),
):
    """
    Returns notification items ordered by section priority then recency.
 
    By default (include_dismissed=false) only active notifications are returned —
    this is what the feed renders.
 
    Pass include_dismissed=true to get ALL notifications including dismissed ones;
    the hook uses this so it can show the 'Restore N dismissed' count and flip
    them back locally without an extra round-trip.
    """
    SECTION_RANK = {
        "Today":                0,
        "Earlier This Week":    1,
        "Intelligence Updates": 2,
        "Opportunity Alerts":   3,
        "System Activity":      4,
    }
    col   = get_notifications_col()
    query: dict[str, Any] = {}
 
    if not include_dismissed:
        query["isDismissed"] = False     # ← only change from v1
 
    if section:   query["section"]  = section
    if priority:  query["priority"] = priority
    if category:  query["category"] = category
 
    docs = await col.find(query).to_list(length=500)
 
    docs.sort(key=lambda d: (
        SECTION_RANK.get(d.get("section", ""), 99),
        d.get("generatedAt", ""),
    ))
 
    result: list[NotificationItemModel] = []
    for doc in docs:
        doc["id"] = str(doc.get("id") or doc["_id"])
        doc.pop("_id", None)
        try:
            result.append(NotificationItemModel(**doc))
        except Exception as e:
            print(f"  ⚠  Skipping malformed notification: {e}")
    return result

@app.patch("/notifications/{notif_id}/important")
async def toggle_notification_important(notif_id: str):
    """
    Toggles isImportant on a notification.
    The hook calls this after the optimistic local update — fire and forget.
    """
    col = get_notifications_col()
 
    # Read current value then flip it
    doc = await col.find_one({"id": notif_id}, {"isImportant": 1})
    if not doc:
        raise HTTPException(status_code=404, detail="Notification not found.")
 
    new_value = not doc.get("isImportant", False)
    await col.update_one(
        {"id": notif_id},
        {"$set": {"isImportant": new_value}},
    )
    return {"success": True, "id": notif_id, "isImportant": new_value}

@app.patch("/notifications/{notif_id}/dismiss")
async def dismiss_notification(notif_id: str):
    """
    Soft-deletes a notification by setting isDismissed=True.
    The item stays in the DB so the 'restore dismissed' feature works.
 
    Why soft delete instead of the existing DELETE route:
      DELETE = permanent, can't restore.
      PATCH /dismiss = reversible, restore flips it back.
    """
    col = get_notifications_col()
    result = await col.update_one(
        {"id": notif_id},
        {"$set": {"isDismissed": True}},
    )
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Notification not found.")
    return {"success": True, "id": notif_id, "isDismissed": True} 

@app.post("/notifications/restore-dismissed")
async def restore_dismissed_notifications():
    """
    Sets isDismissed=False on all notifications.
    Called when the user taps 'Restore N dismissed'.
    """
    col    = get_notifications_col()
    result = await col.update_many(
        {"isDismissed": True},
        {"$set": {"isDismissed": False}},
    )
    return {"success": True, "restored": result.modified_count}

# ── GET /notifications/status ─────────────────────────────────────────────────
@app.get("/notifications/status")
async def notifications_status():
    job      = scheduler.get_job("weekly_notifications")
    next_run = job.next_run_time.isoformat() if job and job.next_run_time else None
    return {
        "running":    notifications_state["running"],
        "lastRun":    notifications_state["last_run"],
        "lastCount":  notifications_state["last_count"],
        "nextRun":    next_run,
        "message": (
            "Generation in progress..."
            if notifications_state["running"]
            else f"Next scheduled run: {next_run or 'unknown'}"
        ),
    }
 
 
# ── POST /notifications/generate ─────────────────────────────────────────────
@app.post("/notifications/generate")
async def force_generate_notifications():
    """Force-trigger the notification pipeline outside of the weekly schedule."""
    count = await generate_notifications_pipeline()
    return {
        "message": "Notification generation complete.",
        "count":   count,
    }
 
 
# ── PATCH /notifications/{id}/read ───────────────────────────────────────────
@app.patch("/notifications/{notif_id}/read")
async def mark_notification_read(notif_id: str):
    col = get_notifications_col()
    await col.update_one({"id": notif_id}, {"$set": {"isRead": True}})
    return {"success": True, "id": notif_id}
 
 
# ── PATCH /notifications/read-all ────────────────────────────────────────────
@app.patch("/notifications/read-all")
async def mark_all_notifications_read():
    col    = get_notifications_col()
    result = await col.update_many({}, {"$set": {"isRead": True}})
    return {"success": True, "updated": result.modified_count}
 
 
# ── DELETE /notifications/{id} ────────────────────────────────────────────────
@app.delete("/notifications/{notif_id}")
async def delete_notification(notif_id: str):
    col = get_notifications_col()
    await col.delete_one({"id": notif_id})
    return {"success": True, "id": notif_id}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ENTRYPOINT
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)