import asyncio
import os
import json
import re
import httpx
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from google import genai
from bs4 import BeautifulSoup
import random

load_dotenv()

# ── Config ─────────────────────────────────────────────────────────────────────
GEMINI_API_KEY1 = os.getenv("GEMINI_API_KEY1")
GEMINI_API_KEY2 = os.getenv("GEMINI_API_KEY2")
GEMINI_API_KEY = random.choice([key for key in [GEMINI_API_KEY1, GEMINI_API_KEY2] if key])  # pick a random key if multiple are set
TELEGRAM_API_ID   = int(os.getenv("TELEGRAM_API_ID", "0"))
TELEGRAM_API_HASH = os.getenv("TELEGRAM_API_HASH", "")
TELEGRAM_PHONE    = os.getenv("TELEGRAM_PHONE", "")   # e.g. +2348012345678

MIN_RESULTS = 15
BATCH_SIZE  = 10

# ── Gemini setup ───────────────────────────────────────────────────────────────
gemini = genai.Client(api_key=GEMINI_API_KEY)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TELEGRAM CHANNELS — add any new channel username here and it will be scraped
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
TELEGRAM_CHANNELS = [
    # ── Nigerian tech / internship focused ────────────────────────────────────
    "jobnetworkng",        # Nigeria Tech Talent
    "Freshersjobsupdates",        # Tech Internships Nigeria
    "ingressive4good",          # Ingressive For Good
    "techies_in_nigeria",       # Techies in Nigeria
    "nigeriatechjobs",          # Nigeria Tech Jobs
    "lagostechjobs",            # Lagos Tech Jobs
    "techJobsNG",               # Tech Jobs NG
    "devjobsng",                # Dev Jobs Nigeria

    # ── Pan-African / broader remote ──────────────────────────────────────────
    "africatechjobs",           # Africa Tech Jobs
    "remotejobsafrica",         # Remote Jobs Africa

    # ── Add your own below — just paste the channel username (no @) ──────────
    # "yourchannel",
]

# How many recent messages to scan per channel
TELEGRAM_MSG_LIMIT = 100

# ── Location rank (lower = shown first) ───────────────────────────────────────
LOCATION_RANK = {
    "Nigeria Remote": 0,
    "Nigeria Onsite": 1,
    "Remote":         2,
    "Other":          3,
    "Unknown":        4,
}

# ── Confidence rank (lower = shown first) ─────────────────────────────────────
# Gemini returns a confidence field so borderline posts aren't thrown away
CONFIDENCE_RANK = {
    "high":   0,
    "medium": 1,
    "low":    2,
}


# ──────────────────────────────────────────────────────────────────────────────
# SOURCE 1 — Remotive
# ──────────────────────────────────────────────────────────────────────────────
REMOTIVE_URL = "https://remotive.com/api/remote-jobs"
REMOTIVE_CATEGORIES = ["software-dev", "mobile"]

ROLE_KEYWORDS = [
    "frontend", "front-end", "front end",
    "backend", "back-end", "back end",
    "fullstack", "full-stack", "full stack",
    "mobile", "android", "ios", "react native", "flutter",
    "intern", "internship", "junior", "entry",
    "developer", "engineer", "programmer",   # broader net
]

async def fetch_remotive(client: httpx.AsyncClient) -> list[dict]:
    listings = []
    seen: set[str] = set()

    for category in REMOTIVE_CATEGORIES:
        try:
            resp = await client.get(
                REMOTIVE_URL,
                params={"category": category, "limit": 50},
                timeout=15,
            )
            resp.raise_for_status()
            jobs = resp.json().get("jobs", [])

            for job in jobs:
                job_id = str(job.get("id", ""))
                if job_id in seen:
                    continue

                title = (job.get("job_title") or "").lower()
                tags  = " ".join(job.get("tags") or []).lower()
                desc  = (job.get("description") or "").lower()
                combined = f"{title} {tags} {desc}"

                if not any(kw in combined for kw in ROLE_KEYWORDS):
                    continue

                seen.add(job_id)
                listings.append({
                    "id":         job_id,
                    "source":     "Remotive",
                    "text":       f"{job.get('job_title')} at {job.get('company_name')}\n"
                                  f"Location: {job.get('candidate_required_location', 'Remote')}\n"
                                  f"Salary: {job.get('salary') or 'Not stated'}\n"
                                  f"Tags: {', '.join(job.get('tags') or [])}\n"
                                  f"Posted: {job.get('publication_date', '')}\n"
                                  f"URL: {job.get('url', '')}",
                    "created_at": job.get("publication_date", ""),
                    "url":        job.get("url", ""),
                    "user":       job.get("company_name", "Unknown"),
                    "username":   job.get("company_name", "unknown"),
                })

        except Exception as e:
            print(f"  ⚠  Remotive [{category}] error — {e}")

    print(f"  ↳ Remotive: {len(listings)} listings")
    return listings


# ──────────────────────────────────────────────────────────────────────────────
# SOURCE 2 — Jobberman
# ──────────────────────────────────────────────────────────────────────────────
JOBBERMAN_QUERIES = [
    "frontend internship",
    "backend internship",
    "fullstack internship",
    "mobile developer internship",
    "software developer internship",
    "junior developer",
]

async def fetch_jobberman(client: httpx.AsyncClient) -> list[dict]:
    listings = []
    seen: set[str] = set()
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
    }

    for query in JOBBERMAN_QUERIES:
        slug = query.replace(" ", "+")
        url  = f"https://www.jobberman.com/jobs?q={slug}&l=Nigeria"
        try:
            resp = await client.get(url, headers=headers, timeout=20, follow_redirects=True)
            soup = BeautifulSoup(resp.text, "html.parser")

            cards = (
                soup.select("div[class*='listing-item']")
                or soup.select("li[class*='job-card']")
                or soup.select("div[class*='job-item']")
                or soup.select("article[class*='job']")
            )

            if cards:
                for card in cards[:10]:
                    title_el = card.select_one("h2, h3, [class*='title']")
                    comp_el  = card.select_one("[class*='company']")
                    link_el  = card.select_one("a")
                    title    = title_el.get_text(strip=True) if title_el else "Unknown"
                    company  = comp_el.get_text(strip=True)  if comp_el  else "Unknown"
                    href     = link_el.get("href", "")        if link_el  else ""
                    job_id   = href
                    if not job_id or job_id in seen:
                        continue
                    seen.add(job_id)
                    full_url = href if href.startswith("http") else f"https://www.jobberman.com{href}"
                    listings.append({
                        "id":         job_id,
                        "source":     "Jobberman",
                        "text":       f"{title} at {company}\nLocation: Nigeria\nURL: {full_url}",
                        "created_at": "",
                        "url":        full_url,
                        "user":       company,
                        "username":   "jobberman",
                    })
            else:
                links = soup.select("a[href*='/jobs/']")
                for a in links[:15]:
                    href  = a.get("href", "")
                    title = a.get_text(strip=True)
                    if not title or len(title) < 5:
                        continue
                    job_id = href
                    if job_id in seen:
                        continue
                    seen.add(job_id)
                    full_url = href if href.startswith("http") else f"https://www.jobberman.com{href}"
                    listings.append({
                        "id":         job_id,
                        "source":     "Jobberman",
                        "text":       f"{title}\nLocation: Nigeria\nURL: {full_url}",
                        "created_at": "",
                        "url":        full_url,
                        "user":       "Jobberman",
                        "username":   "jobberman",
                    })

        except Exception as e:
            print(f"  ⚠  Jobberman [{query}] error — {e}")

        await asyncio.sleep(2)

    print(f"  ↳ Jobberman: {len(listings)} listings")
    return listings


# ──────────────────────────────────────────────────────────────────────────────
# SOURCE 3 — MyJobMag
# ──────────────────────────────────────────────────────────────────────────────
MYJOBMAG_QUERIES = [
    "frontend developer",
    "backend developer",
    "software developer",
    "mobile developer",
    "junior developer",
    "software intern",
]

async def fetch_myjobmag(client: httpx.AsyncClient) -> list[dict]:
    listings = []
    seen: set[str] = set()
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
    }

    for query in MYJOBMAG_QUERIES:
        slug = query.replace(" ", "+")
        url  = f"https://www.myjobmag.com/search-jobs?keywords={slug}&location=Nigeria"
        try:
            resp = await client.get(url, headers=headers, timeout=20, follow_redirects=True)
            soup = BeautifulSoup(resp.text, "html.parser")

            cards = (
                soup.select("div.job-list-item")
                or soup.select("li.job-item")
                or soup.select("article[class*='job']")
                or soup.select("div[class*='job-card']")
            )
            links = soup.select("a[href*='/job/']") if not cards else []

            targets = cards if cards else links
            for el in targets[:10]:
                if el.name == "a":
                    href  = el.get("href", "")
                    title = el.get_text(strip=True)
                else:
                    link_el  = el.select_one("a")
                    href     = link_el.get("href", "") if link_el else ""
                    title_el = el.select_one("h2, h3, [class*='title']")
                    title    = title_el.get_text(strip=True) if title_el else el.get_text(strip=True)[:80]

                if not title or len(title) < 5:
                    continue
                job_id = href
                if not job_id or job_id in seen:
                    continue
                seen.add(job_id)
                full_url = href if href.startswith("http") else f"https://www.myjobmag.com{href}"
                listings.append({
                    "id":         job_id,
                    "source":     "MyJobMag",
                    "text":       f"{title}\nLocation: Nigeria\nURL: {full_url}",
                    "created_at": "",
                    "url":        full_url,
                    "user":       "MyJobMag",
                    "username":   "myjobmag",
                })

        except Exception as e:
            print(f"  ⚠  MyJobMag [{query}] error — {e}")

        await asyncio.sleep(2)

    print(f"  ↳ MyJobMag: {len(listings)} listings")
    return listings


# ──────────────────────────────────────────────────────────────────────────────
# SOURCE 4 — Himalayas
# ──────────────────────────────────────────────────────────────────────────────
async def fetch_himalayas(client: httpx.AsyncClient) -> list[dict]:
    listings = []
    seen: set[str] = set()
    queries = [
        "frontend intern", "backend intern",
        "fullstack intern", "mobile developer intern",
        "junior frontend", "junior backend", "junior developer",
    ]
    headers = {"Accept": "application/json"}

    for q in queries:
        try:
            resp = await client.get(
                "https://himalayas.app/jobs/api",
                params={"q": q, "limit": 20},
                headers=headers,
                timeout=15,
            )
            resp.raise_for_status()
            jobs = resp.json().get("jobs", [])

            for job in jobs:
                job_id = str(job.get("id") or job.get("slug", ""))
                if job_id in seen:
                    continue
                seen.add(job_id)
                listings.append({
                    "id":         job_id,
                    "source":     "Himalayas",
                    "text":       f"{job.get('title')} at {job.get('company', {}).get('name', 'Unknown')}\n"
                                  f"Location: {job.get('locationRestrictions') or 'Remote'}\n"
                                  f"Salary: {job.get('salaryRange') or 'Not stated'}\n"
                                  f"URL: https://himalayas.app/jobs/{job.get('slug', '')}",
                    "created_at": job.get("createdAt", ""),
                    "url":        f"https://himalayas.app/jobs/{job.get('slug', '')}",
                    "user":       job.get("company", {}).get("name", "Unknown"),
                    "username":   "himalayas",
                })

        except Exception as e:
            print(f"  ⚠  Himalayas [{q}] error — {e}")

        await asyncio.sleep(1)

    print(f"  ↳ Himalayas: {len(listings)} listings")
    return listings


# ──────────────────────────────────────────────────────────────────────────────
# SOURCE 5 — Telegram
# ──────────────────────────────────────────────────────────────────────────────
# Requires:  pip install telethon
# First run: creates a session file (scraper.session) — Telegram will SMS you a
#            one-time code.  After that, the session is reused silently.
#
# Get your API credentials (free) at: https://my.telegram.org/apps
# Set in .env:
#   TELEGRAM_API_ID=12345678
#   TELEGRAM_API_HASH=abcdef1234567890abcdef1234567890
#   TELEGRAM_PHONE=+2348012345678
# ──────────────────────────────────────────────────────────────────────────────

# Keywords that suggest a message is a job/internship post
TELEGRAM_ROLE_KEYWORDS = [
    "intern", "internship",
    "junior", "entry level", "entry-level",
    "frontend", "front-end",
    "backend", "back-end",
    "fullstack", "full stack", "full-stack",
    "mobile developer", "react native", "flutter",
    "android developer", "ios developer",
    "software developer", "software engineer",
    "we are hiring", "we're hiring", "now hiring",
    "open role", "open position", "job opening",
    "apply", "application",
]

def _looks_like_job_post(text: str) -> bool:
    lower = text.lower()
    return any(kw in lower for kw in TELEGRAM_ROLE_KEYWORDS)


async def fetch_telegram() -> list[dict]:
    """
    Scrape the channels listed in TELEGRAM_CHANNELS.
    Returns an empty list (with a warning) if Telethon is not installed or
    credentials are missing — so the rest of the scraper still works.
    """
    if not TELEGRAM_API_ID or not TELEGRAM_API_HASH or not TELEGRAM_PHONE:
        print("  ↳ Telegram: skipped (TELEGRAM_API_ID / HASH / PHONE not set in .env)")
        return []

    try:
        from telethon import TelegramClient
        from telethon.errors import ChannelInvalidError, UsernameNotOccupiedError
    except ImportError:
        print("  ↳ Telegram: skipped (run: pip install telethon)")
        return []

    listings = []
    seen: set[str] = set()

    client = TelegramClient("scraper", TELEGRAM_API_ID, TELEGRAM_API_HASH)
    await client.start(phone=TELEGRAM_PHONE)

    for channel in TELEGRAM_CHANNELS:
        try:
            entity   = await client.get_entity(channel)
            messages = await client.get_messages(entity, limit=TELEGRAM_MSG_LIMIT)
            found    = 0

            for msg in messages:
                if not msg.text:
                    continue
                if not _looks_like_job_post(msg.text):
                    continue

                msg_id = f"{channel}_{msg.id}"
                if msg_id in seen:
                    continue
                seen.add(msg_id)
                found += 1

                # Build a direct t.me link to the message
                link = f"https://t.me/{channel}/{msg.id}"

                listings.append({
                    "id":         msg_id,
                    "source":     "Telegram",
                    "channel":    channel,
                    "text":       msg.text[:1000],   # cap at 1000 chars for Gemini
                    "created_at": msg.date.isoformat() if msg.date else "",
                    "url":        link,
                    "user":       channel,
                    "username":   channel,
                })

            print(f"    • @{channel}: {found} job-like messages")

        except (ChannelInvalidError, UsernameNotOccupiedError):
            print(f"    ⚠  @{channel}: channel not found / private")
        except Exception as e:
            print(f"    ⚠  @{channel}: {e}")

    await client.disconnect()
    print(f"  ↳ Telegram: {len(listings)} listings across {len(TELEGRAM_CHANNELS)} channels")
    return listings


# ──────────────────────────────────────────────────────────────────────────────
# Aggregate all sources
# ──────────────────────────────────────────────────────────────────────────────
async def scrape_all() -> list[dict]:
    async with httpx.AsyncClient() as client:
        web_results = await asyncio.gather(
            fetch_remotive(client),
            fetch_jobberman(client),
            fetch_myjobmag(client),
            fetch_himalayas(client),
        )

    # Telegram uses its own long-lived client, run separately
    tg_results = await fetch_telegram()

    combined  = []
    seen_ids: set[str] = set()

    for source_list in [*web_results, tg_results]:
        for item in source_list:
            uid = f"{item['source']}:{item['id']}"
            if uid not in seen_ids:
                seen_ids.add(uid)
                combined.append(item)

    return combined


# ──────────────────────────────────────────────────────────────────────────────
# Gemini validation  — LENIENT MODE
#
# Key changes vs original:
#   • is_valid is true for ANYTHING that could plausibly be a real, open
#     tech-related role — including mid/senior if it also says "or junior"
#   • Added  confidence: "high" | "medium" | "low"
#       high   = clearly an internship / junior / entry role
#       medium = real tech role but seniority unclear or slightly senior
#       low    = scraped content looks like a real job but details are thin
#   • Only mark is_valid=false for: spam, error pages, non-tech roles
#     (marketing, sales, writing, admin), or aggregator noise with no job info
# ──────────────────────────────────────────────────────────────────────────────
GEMINI_PROMPT = """You are a LENIENT filter for tech job postings scraped from job boards and Telegram channels.

Your job is to keep anything that could be a real, open software/tech role and only discard obvious non-matches.

MARK is_valid = true if:
✅ The posting is for any software/tech role — frontend, backend, fullstack, mobile, DevOps, data, QA, etc.
✅ The company appears real (not an aggregator homepage or error page)
✅ Seniority does NOT matter — keep junior AND senior AND mid-level roles
✅ "Junior developer", "Entry-level engineer", "Intern", "Graduate developer" all count
✅ If seniority is unclear, keep it — give benefit of the doubt
✅ Telegram posts with vague details but obvious hiring intent → keep

MARK is_valid = false ONLY if:
❌ Pure non-tech role: sales, marketing copywriting, accounting, admin, video editing, writing (with no tech component)
❌ The "post" is clearly not a job: error page, navigation link, "Post a Job" CTA, generic site content
❌ Obvious spam or scam wording
❌ Duplicate of a listing already shown (same company + same role)

For confidence:
- "high"   = clearly internship / junior / entry-level / graduate role
- "medium" = real tech role but mid or senior level — still useful context
- "low"    = real job but details are thin or seniority is ambiguous

For each numbered listing return a JSON array — one object per listing — with EXACTLY these keys:
{
  "index":       <integer — same as the [N] prefix>,
  "is_valid":    true | false,
  "confidence":  "high" | "medium" | "low",
  "reason":      "one sentence only if is_valid is false, else empty string",
  "company":     "company or person name, or null",
  "position":    "exact role title",
  "pay":         "pay/stipend info extracted verbatim, or null",
  "location":    one of → "Nigeria Remote" | "Nigeria Onsite" | "Remote" | "Other" | "Unknown",
  "notes":       "tech stack, duration, deadline, perks — any extra facts",
  "apply_link":  "direct application link found in the listing, or null"
}

Return ONLY the JSON array. No markdown fences, no explanation, no preamble.

LISTINGS:
"""


def validate_batch(batch: list[dict]) -> list[dict]:
    numbered = "\n\n".join(
        f"[{i}] Source: {t['source']} | Channel: {t.get('channel', 'N/A')} | Posted: {t.get('created_at', 'Unknown')}\n{t['text']}\nURL: {t['url']}"
        for i, t in enumerate(batch)
    )

    print("\n" + "─" * 52)
    print("  📤 SENDING TO GEMINI:")
    print(numbered[:2000])
    print("─" * 52)

    try:
        response = gemini.models.generate_content(
            model="gemini-2.5-flash",
            contents=GEMINI_PROMPT + numbered,
        )
        raw = response.text.strip()

        print("\n  📥 GEMINI RAW REPLY:")
        print(raw[:3000])
        print("─" * 52 + "\n")

        if raw.startswith("```"):
            parts = raw.split("```")
            raw   = parts[1] if len(parts) > 1 else raw
            if raw.startswith("json"):
                raw = raw[4:]
        raw = raw.strip()

        parsed: list[dict] = json.loads(raw)

        for item in parsed:
            conf   = item.get("confidence", "?")
            if item.get("is_valid"):
                status = f"✅ VALID [{conf}]"
            else:
                status = f"❌ INVALID — {item.get('reason', '')}"
            print(f"  [{item.get('index')}] {status}")

        valid = []
        for item in parsed:
            if item.get("is_valid"):
                idx = item["index"]
                if 0 <= idx < len(batch):
                    valid.append({**batch[idx], **item})
        return valid

    except json.JSONDecodeError as e:
        print(f"  ⚠  Gemini JSON parse error: {e}")
        return []
    except Exception as e:
        print(f"  ⚠  Gemini error: {e}")
        return []


def validate_all(listings: list[dict]) -> list[dict]:
    valid = []
    for i in range(0, len(listings), BATCH_SIZE):
        batch = listings[i : i + BATCH_SIZE]
        print(f"  Validating listings {i+1}–{i+len(batch)}...")
        valid.extend(validate_batch(batch))
    return valid


# ──────────────────────────────────────────────────────────────────────────────
# Ranking — Nigeria first, then confidence (high before medium/low)
# ──────────────────────────────────────────────────────────────────────────────
def rank(results: list[dict]) -> list[dict]:
    return sorted(
        results,
        key=lambda x: (
            LOCATION_RANK.get(x.get("location", "Unknown"), 4),
            CONFIDENCE_RANK.get(x.get("confidence", "low"), 2),
        ),
    )


# ──────────────────────────────────────────────────────────────────────────────
# Printing
# ──────────────────────────────────────────────────────────────────────────────
LOCATION_BADGE = {
    "Nigeria Remote": "🇳🇬 Nigeria Remote ⭐",
    "Nigeria Onsite": "🇳🇬 Nigeria Onsite",
    "Remote":         "🌍 Remote",
    "Other":          "📍 Other",
    "Unknown":        "❓ Unknown",
}

SOURCE_BADGE = {
    "Remotive":  "🔵 Remotive",
    "Jobberman": "🟢 Jobberman",
    "MyJobMag":  "🟠 MyJobMag",
    "Himalayas": "🟣 Himalayas",
    "Telegram":  "✈️  Telegram",
}

CONFIDENCE_BADGE = {
    "high":   "🟢 High  (intern/junior/entry)",
    "medium": "🟡 Medium (mid/senior — still useful)",
    "low":    "🔴 Low   (thin details / unclear level)",
}

def print_results(results: list[dict]) -> None:
    divider = "━" * 52
    print(f"\n{divider}")
    if not results:
        print("  ❌  No valid listings found.")
        print(divider)
        return

    # Summary by confidence
    high   = sum(1 for r in results if r.get("confidence") == "high")
    medium = sum(1 for r in results if r.get("confidence") == "medium")
    low    = sum(1 for r in results if r.get("confidence") == "low")

    print(f"  ✅  {len(results)} listing(s) found")
    print(f"      🟢 {high} high-confidence  |  🟡 {medium} medium  |  🔴 {low} low")
    print(divider)

    for i, r in enumerate(results, 1):
        loc    = r.get("location", "Unknown")
        badge  = LOCATION_BADGE.get(loc, loc)
        pay    = r.get("pay")   or "Not stated"
        notes  = r.get("notes") or "N/A"
        link   = r.get("apply_link") or r.get("url", "N/A")
        source = SOURCE_BADGE.get(r.get("source", ""), r.get("source", "Unknown"))
        conf   = CONFIDENCE_BADGE.get(r.get("confidence", "low"), r.get("confidence", ""))
        chan   = f"  ✈️  Channel   : @{r['channel']}\n" if r.get("channel") else ""

        print(f"\n[{i}] {divider}")
        print(f"  🏢  Company   : {r.get('company') or 'Unknown'}")
        print(f"  💼  Position  : {r.get('position') or 'Unknown'}")
        print(f"  💰  Pay       : {pay}")
        print(f"  📍  Location  : {badge}")
        print(f"  🎯  Confidence: {conf}")
        print(f"  📝  Notes     : {notes}")
        print(f"  🔗  Apply     : {link}")
        print(f"  🕐  Posted    : {r.get('created_at', 'Unknown')}")
        print(f"  📡  Source    : {source}")
        if chan:
            print(chan, end="")

    print(f"\n{divider}\n")


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────
async def main() -> None:
    print("\n🚀 Internship Scraper — Remotive + Jobberman + MyJobMag + Himalayas + Telegram + Gemini")
    print("━" * 52)

    print("\n📡 Fetching from all sources...")
    listings = await scrape_all()
    print(f"\n  📦 {len(listings)} total unique listings collected")

    if not listings:
        print("  ❌ No listings found from any source.")
        return

    print("\n  🤖 Sending to Gemini for validation...")
    valid = validate_all(listings)
    print(f"  ✅ {len(valid)} valid listings after filtering")

    print_results(rank(valid))


if __name__ == "__main__":
    asyncio.run(main())