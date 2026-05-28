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
from fastapi.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection
from pydantic import BaseModel
from google import genai

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
    running:   bool
    lastRun:   Optional[str] = None
    lastSaved: Optional[int] = None
    nextRun:   Optional[str] = None
    message:   str

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
    db.signals = col
    notif_col = db.client[DB_NAME]["notifications"]
    await notif_col.create_index("section")
    await notif_col.create_index("priority")
    await notif_col.create_index("isRead")
    await notif_col.create_index("generatedAt")
    print(f"✅ MongoDB connected → {DB_NAME}.signals")


async def close_db() -> None:
    if db.client:
        db.client.close()


def get_signals() -> AsyncIOMotorCollection:
    if db.signals is None:
        raise RuntimeError("Database not initialised")
    return db.signals


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SCRAPE STATE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
scrape_state: dict[str, Any] = {
    "running":    False,
    "last_run":   None,
    "last_saved": None,
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
    ai_match_score    = EXTRACTION_CONFIDENCE_SCORE.get(extraction_confidence, 65)
    ai_summary        = notes or _truncate(raw_text, 220) or "No summary available."
    source_post_prev  = _truncate(raw_text, 280)
    source_handle     = channel or username or source
    source_metadata   = [s for s in [source, channel, username, location_raw] if s and s != "Unknown"]

    return {
        # ── Identity ──────────────────────────────────────────────────────────
        "scrapedId":           build_scraped_id(item),
        "platform":            source,
        "status":              PipelineStage.new.value,
        "addedAt":             datetime.now(timezone.utc).isoformat(),

        # ── IntelligenceSignal fields ─────────────────────────────────────────
        "role":                role,
        "company":             company,
        "location":            location_raw,
        "aiMatchScore":        ai_match_score,
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
        "relatedIds":          [],          # populated post-insert if needed

        # ── Extras ────────────────────────────────────────────────────────────
        "pay":       pay,
        "applyLink": apply_link,
    }


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

TELEGRAM_CHANNELS = [
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


async def fetch_remotive(client: httpx.AsyncClient) -> list[dict]:
    listings: list[dict] = []
    seen: set[str] = set()
    for category in ["software-dev", "mobile"]:
        try:
            resp = await client.get(
                "https://remotive.com/api/remote-jobs",
                params={"category": category, "limit": 50},
                timeout=15,
            )
            resp.raise_for_status()
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
            resp  = await client.get(url, headers=headers, timeout=20, follow_redirects=True)
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
            resp  = await client.get(url, headers=headers, timeout=20, follow_redirects=True)
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
            resp = await client.get(
                "https://himalayas.app/jobs/api",
                params={"q": q, "limit": 20},
                headers={"Accept": "application/json"},
                timeout=15,
            )
            resp.raise_for_status()
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


async def fetch_telegram() -> list[dict]:
    if not (TELEGRAM_API_ID and TELEGRAM_API_HASH and TELEGRAM_PHONE):
        print("  ↳ Telegram: skipped (credentials not set)")
        return []
    try:
        from telethon import TelegramClient
        from telethon.errors import ChannelInvalidError, UsernameNotOccupiedError
    except ImportError:
        print("  ↳ Telegram: skipped (pip install telethon)")
        return []

    listings: list[dict] = []
    seen: set[str] = set()
    tg = TelegramClient("scraper", TELEGRAM_API_ID, TELEGRAM_API_HASH)
    await tg.start(phone=TELEGRAM_PHONE)
    for channel in TELEGRAM_CHANNELS:
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
                    "id": mid, "source": "Telegram", "channel": channel,
                    "text": msg.text[:1000],
                    "created_at": msg.date.isoformat() if msg.date else "",
                    "url": f"https://t.me/{channel}/{msg.id}",
                    "user": channel, "username": channel,
                })
            print(f"    • @{channel}: {found}")
        except (ChannelInvalidError, UsernameNotOccupiedError):
            print(f"    ⚠  @{channel}: not found / private")
        except Exception as e:
            print(f"    ⚠  @{channel}: {e}")
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
Return ONLY a JSON array, no markdown fences, no commentary.
One object per listing, indexed from 0:

{
  "index":              <integer>,
  "is_valid":           true | false,
  "confidence":         "High" | "Medium" | "Low",
  "source_confidence":  "High" | "Medium" | "Low",
  "reason":             "one sentence if invalid, else empty string",
  "company":            "company name or null",
  "position":           "exact role title",
  "pay":                "verbatim pay info or null",
  "location":           "Nigeria Remote" | "Nigeria Onsite" | "Remote" | "Other" | "Unknown",
  "role_mode":          "Remote" | "Hybrid" | "On-site",
  "role_type":          "Software Engineering" | "Mobile" | "Data" | "DevOps" | "QA" | "Design" | "Other",
  "application_status": "Open" | "Closing soon" | "Unknown",
  "skill_tags":         ["Tag1", "Tag2", ...],
  "skill_alignment":    "Skill1, Skill2, ...",
  "relevance_reason":   "Why this matters to a Nigerian dev",
  "notes":              "Short summary for feed card",
  "apply_link":         "url or null"
}

LISTINGS:
"""


def _next_client() -> tuple[genai.Client, int]:
    global _client_index
    idx    = _client_index % len(_gemini_clients)
    client = _gemini_clients[idx]
    _client_index += 1
    return client, idx


def _parse_gemini_response(raw: str, batch: list[dict]) -> list[dict]:
    """Strip markdown fences and parse Gemini JSON into enriched items."""
    raw = raw.strip()
    if raw.startswith("```"):
        parts = raw.split("```")
        raw   = parts[1] if len(parts) > 1 else raw
        if raw.startswith("json"):
            raw = raw[4:]
    raw = raw.strip()

    parsed: list[dict] = json.loads(raw)   # raises JSONDecodeError on bad output
    results: list[dict] = []
    for item in parsed:
        idx_val = item.get("index")
        if not item.get("is_valid") or idx_val is None or not (0 <= idx_val < len(batch)):
            continue
        enriched = {
            **batch[idx_val],
            "company":            item.get("company"),
            "position":           item.get("position"),
            "pay":                item.get("pay"),
            "location":           item.get("location", "Unknown"),
            "role_mode":          item.get("role_mode", "Remote"),
            "role_type":          item.get("role_type", "Software Engineering"),
            "application_status": item.get("application_status", "Unknown"),
            "skill_tags":         item.get("skill_tags", []),
            "skill_alignment":    item.get("skill_alignment", ""),
            "relevance_reason":   item.get("relevance_reason", ""),
            "notes":              item.get("notes", ""),
            "apply_link":         item.get("apply_link"),
            "confidence":         item.get("confidence", "Medium"),
            "source_confidence":  item.get("source_confidence", "Medium"),
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
        Raises JSONDecodeError immediately on bad JSON (not retriable).
        """
        for _ in range(n_keys):
            client, idx = _next_client()
            key_label   = f"key {idx + 1}/{n_keys}"
            try:
                response = client.models.generate_content(
                    model="gemini-2.5-flash",
                    contents=prompt,
                )
                results = _parse_gemini_response(response.text, batch)
                print(f"    ✓ Batch validated ({key_label})")
                return results

            except json.JSONDecodeError as e:
                # Malformed JSON — retrying won't help; bail immediately
                print(f"  ⚠  Gemini JSON parse error ({key_label}): {e}")
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
        except json.JSONDecodeError:
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

    scrape_state["running"] = True
    col = get_signals()
    saved = skipped = 0

    try:
        print(f"\n🕐 [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Scrape started")

        print("📡 Fetching from all sources...")
        raw = await scrape_all()
        print(f"  📦 {len(raw)} total unique raw listings")

        if not raw:
            print("  ❌ No listings found.")
            return {"saved": 0, "skipped": 0}

        print("🔎 Checking DB for already-scraped duplicates...")
        fresh = await filter_already_scraped(raw)
        print(f"  📬 {len(fresh)} new listings (dropped {len(raw) - len(fresh)} dupes)")

        if not fresh:
            print("  ✅ Nothing new to process.")
            scrape_state["last_run"]   = datetime.now(timezone.utc).isoformat()
            scrape_state["last_saved"] = 0
            return {"saved": 0, "skipped": len(raw)}

        print("🤖 Validating + enriching with Gemini — saving each batch immediately...")
        total_batches = -(-len(fresh) // BATCH_SIZE)   # ceiling division

        for batch_num, i in enumerate(range(0, len(fresh), BATCH_SIZE), start=1):
            batch = fresh[i : i + BATCH_SIZE]
            print(f"  [{batch_num}/{total_batches}] Validating items {i+1}–{i+len(batch)} of {len(fresh)}...")

            # ── Gemini (may backoff+retry internally; raises RuntimeError if unrecoverable) ──
            validated = await validate_batch(batch)

            if not validated:
                print(f"    ↳ 0 valid in this batch — skipping DB write")
                skipped += len(batch)
                continue

            # ── Write immediately — no waiting for the rest of the batches ──
            b_saved, b_skipped = await save_batch(col, validated)
            saved   += b_saved
            skipped += b_skipped
            print(f"    ↳ {b_saved} saved, {b_skipped} skipped  (running total: {saved} saved)")

            # Update state after every batch so /scrape/status stays fresh
            scrape_state["last_saved"] = saved

        scrape_state["last_run"]   = datetime.now(timezone.utc).isoformat()
        scrape_state["last_saved"] = saved
        print(f"✅ Pipeline done — saved {saved}, skipped {skipped}\n")
        return {"saved": saved, "skipped": skipped}

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
    scheduler.start()
    notif_next = scheduler.get_job("weekly_notifications").next_run_time
    print(f"🔔 Notification cron — next run: {notif_next.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    
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

# ── GET /signals ──────────────────────────────────────────────────────────────
@app.get("/signals", response_model=PaginatedSignals)
async def get_signals_route(
    page:                int                        = Query(default=1,    ge=1),
    limit:               int                        = Query(default=100,  ge=1, le=200),
    status:              Optional[PipelineStage]    = Query(default=None),
    platform:            Optional[str]              = Query(default=None),
    location:            Optional[str]              = Query(default=None),
    isSaved:             Optional[bool]            = Query(default=None),
    role_mode:           Optional[RoleMode]         = Query(default=None),
    role_type:           Optional[str]              = Query(default=None),
    application_status:  Optional[ApplicationStatus]= Query(default=None),
    extraction_confidence: Optional[ConfidenceLevel]= Query(default=None),
    source_confidence:   Optional[ConfidenceLevel]  = Query(default=None),
    skill_tag:           Optional[str]              = Query(default=None, description="Filter by a skill tag (case-insensitive substring)"),
    sort:                str                        = Query(default="newest", description="newest | score"),
):
    col   = get_signals()
    skip  = (page - 1) * limit
    query: dict[str, Any] = {}

    if status:               query["status"]               = status.value
    if platform:             query["platform"]             = platform
    if location:             query["location"]             = location
    if isSaved is not None:  query["isSaved"]              = isSaved
    if role_mode:            query["roleMode"]             = role_mode.value
    if role_type:            query["roleType"]             = role_type
    if application_status:   query["applicationStatus"]   = application_status.value
    if extraction_confidence: query["extractionConfidence"] = extraction_confidence.value
    if source_confidence:    query["sourceConfidence"]     = source_confidence.value
    if skill_tag:
        # Case-insensitive substring match inside the skillTags array
        query["skillTags"] = {"$elemMatch": {"$regex": skill_tag, "$options": "i"}}

    sort_fields = (
        [("aiMatchScore", -1), ("_id", -1)] if sort == "score"
        else [("_id", -1)]
    )

    docs, total = await asyncio.gather(
        col.find(query).sort(sort_fields).skip(skip).limit(limit).to_list(length=limit),
        col.count_documents(query),
    )

    return PaginatedSignals(
        signals = [doc_to_signal(d) for d in docs],
        total   = total,
        page    = page,
        pages   = -(-total // limit),
        limit   = limit,
    )


# ── GET /signals/{id} ─────────────────────────────────────────────────────────
@app.get("/signals/{id}", response_model=IntelligenceSignal)
async def get_signal(id: str):
    col = get_signals()
    doc = await col.find_one({"_id": valid_object_id(id)})
    if not doc:
        raise HTTPException(status_code=404, detail="Signal not found.")
    return doc_to_signal(doc)


# ── GET /signals/{id}/related ───────────────────────────────────────────────────
@app.get("/signals/{id}/related", response_model=list[IntelligenceSignal])
async def get_related_signals(id: str):
    col = get_signals()
    doc = await col.find_one({"_id": valid_object_id(id)}, {"relatedIds": 1})
    if not doc:
        raise HTTPException(status_code=404, detail="Signal not found.")
    related_ids = doc.get("relatedIds", [])
    if not related_ids:
        return []
    related_docs = await col.find({"_id": {"$in": [valid_object_id(rid) for rid in related_ids]}}).to_list(length=len(related_ids))
    return [doc_to_signal(d) for d in related_docs]

# ── PATCH /signals/{id} ───────────────────────────────────────────────────────
@app.patch("/signals/{signal_id}", response_model=IntelligenceSignal)
async def update_signal_status(signal_id: str, body: StatusUpdate):
    col = get_signals()
    oid = valid_object_id(signal_id)
    print(f"Updating signal {signal_id} to status '{body.status.value}' (isSaved={body.status.value == 'saved'})")

    update: dict[str, object] = {
        "status": body.status.value
    }

    if body.status.value == "saved":
        update["isSaved"] = True
    elif body.status.value == "new":
        update["isSaved"] = False

    updated = await col.find_one_and_update(
        {"_id": oid},
        {"$set": update},
        return_document=ReturnDocument.AFTER,
    )

    if not updated:
        raise HTTPException(status_code=404, detail="Signal not found.")

    return doc_to_signal(updated)

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
        running   = scrape_state["running"],
        lastRun   = scrape_state["last_run"],
        lastSaved = scrape_state["last_saved"],
        nextRun   = next_run,
        message   = (
            "Scrape in progress..." if scrape_state["running"]
            else f"Next scheduled run: {next_run or 'unknown'}"
        ),
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