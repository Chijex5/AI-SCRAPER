"""
rescore.py — Standalone score correction script for the Jobless DB
===================================================================
Runs independently from the FastAPI backend.

Usage:
  # Correct every signal already in MongoDB
  python rescore.py

  # Score a single signal on the spot (callable from anywhere)
  from rescore import score_signal
  score, breakdown = score_signal(doc)
  print(score, breakdown)

  # Or pass raw kwargs for a quick ad-hoc check
  from rescore import score_signal
  score, breakdown = score_signal({
      "location":           "Nigeria Remote",
      "extractionConfidence": "High",
      "roleMode":           "Remote",
      "roleType":           "Software Engineering",
      "applicationStatus":  "Open",
      "addedAt":            "2025-05-25T10:00:00+00:00",
      "skillTags":          ["React", "Node.js", "TypeScript"],
      "aiSummary":          "Junior frontend intern needed...",
      "originalSourceText": "We are hiring an intern...",
  })
"""

from __future__ import annotations

import asyncio
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient

load_dotenv()

MONGODB_URI = os.getenv("MONGODB_URI", "")
DB_NAME     = os.getenv("DB_NAME", "jobless")

# ─────────────────────────────────────────────────────────────────────────────
# SCORING WEIGHTS  (must sum to 100)
# ─────────────────────────────────────────────────────────────────────────────
#
# What makes a "perfect" signal for THIS user (a Nigerian dev seeking entry-level work):
#   ✅  Seniority →  Intern / Junior / Entry-level / "Y-level" is the #1 thing
#   ✅  Pay fit   →  intern stipend / modest pay = good;  ₦1m "professional" pay = NOT for me
#   ✅  Location  →  Nigeria is a bonus, but ANY remote role is welcome ("others are ok")
#   ✅  Role mode →  Remote > Hybrid > On-site
#   ✅  Freshness →  fresher is better; degrades over ~14 days
#   ✅  Role type →  Software Engineering / Mobile preferred
#   ✅  Status    →  Open > Unknown >> Closing soon (still valuable)
#   ✅  Richness  →  more skill tags = more signal that Gemini got good data
#   ✅  Source    →  High confidence sources are more trustworthy
#
# Two changes vs. the old model, driven by user feedback:
#   1. Seniority + a NEW pay-fit signal now dominate, because the make-or-break
#      question is "is this an entry-level role or a senior/professional one?".
#   2. A clearly senior / high-pay ("professional ₦1m") role gets an explicit
#      LEVEL PENALTY applied AFTER the weighted sum, so it lands well below 50
#      no matter how remote / fresh / Nigerian it is. (See SENIOR_PAY_THRESHOLD.)
#
WEIGHTS = {
    "seniority":  28,   # intern/junior/entry-level — the single biggest factor now
    "location":   18,   # Nigeria is a bonus, but remote-anywhere is fine
    "pay_fit":    14,   # NEW: low/stipend pay = good, ₦1m+ = professional = bad
    "freshness":  13,   # age of the listing (capped at ~14 days)
    "role_mode":  12,   # remote > hybrid > on-site
    "role_type":   5,   # software / mobile preferred
    "status":      4,   # open > unknown > closing
    "richness":    3,   # skill tag count — proxy for data quality
    "source":      3,   # source confidence
}
assert sum(WEIGHTS.values()) == 100, "Weights must sum to 100"

# Pay at or above this NGN-equivalent monthly magnitude reads as a senior /
# "professional" role rather than an intern / entry-level one. The user called
# out "₦1m" explicitly as the kind of job they do NOT want to be matched against.
SENIOR_PAY_THRESHOLD = 1_000_000

# Rough FX multipliers to convert a foreign-currency figure to an NGN-equivalent
# magnitude so the same thresholds work regardless of how pay was quoted.
_FX_TO_NGN = {"usd": 1500.0, "eur": 1700.0, "gbp": 2000.0}

# ─────────────────────────────────────────────────────────────────────────────
# INTERN / JUNIOR KEYWORDS  (checked in role title + summary + raw text)
# ─────────────────────────────────────────────────────────────────────────────
SENIORITY_KEYWORDS = [
    "intern", "internship", "interns",
    "junior", "jr.",
    "entry level", "entry-level",
    "graduate", "fresh graduate", "nysc",
    "trainee", "apprentice",
    "0-1 year", "0-2 year", "1 year",
    "0-1 years", "0-2 years", "1 years",
    "new grad", "recent grad",
    "associate",
]

SENIOR_KEYWORDS = [
    "senior", "sr.", "lead", "principal",
    "staff", "architect", "head of",
    "director", "vp of", "chief",
    "5+ year", "7+ year", "10+ year",
    "5 years", "7 years", "10 years",
]

# Words that signal the pay is intern-grade / not a professional salary.
_LOW_PAY_KEYWORDS = ["unpaid", "volunteer", "pro bono", "stipend", "allowance", "no pay"]


def parse_pay(pay: Any) -> tuple[Optional[float], str, str]:
    """
    Best-effort parse of a free-text pay string into an NGN-equivalent magnitude.

    Returns (amount, currency, label):
      amount   — float NGN-equivalent (top of a range), or
                 0.0 for explicit unpaid/stipend language, or
                 None if no pay was stated / nothing parseable.
      currency — "NGN" | "USD" | "EUR" | "GBP" | "" (none/unknown).
      label    — short human description for the breakdown.

    Examples it handles: "₦1,000,000", "1m", "500k/month", "₦250,000 - ₦400,000",
    "$2000", "Unpaid internship", "Competitive".
    """
    if not pay or not str(pay).strip():
        return None, "", "not stated"

    s = str(pay).lower()

    if any(kw in s for kw in _LOW_PAY_KEYWORDS):
        return 0.0, "", "stipend/unpaid"

    # Detect currency → NGN multiplier (default: assume the figure is already NGN)
    if "$" in s or "usd" in s or "dollar" in s:
        currency, mult = "USD", _FX_TO_NGN["usd"]
    elif "€" in s or "eur" in s or "euro" in s:
        currency, mult = "EUR", _FX_TO_NGN["eur"]
    elif "£" in s or "gbp" in s or "pound" in s:
        currency, mult = "GBP", _FX_TO_NGN["gbp"]
    else:
        currency, mult = "NGN", 1.0

    # Pull out every number, honouring k / m suffixes and comma grouping.
    amounts: list[float] = []
    for num, suf in re.findall(r"(\d[\d,]*\.?\d*)\s*([km])?", s):
        cleaned = num.replace(",", "")
        if not cleaned or cleaned == ".":
            continue
        try:
            val = float(cleaned)
        except ValueError:
            continue
        if suf == "k":
            val *= 1_000
        elif suf == "m":
            val *= 1_000_000
        if val > 0:
            amounts.append(val)

    if not amounts:
        return None, "", "unparseable"

    # Use the top of any range (the most optimistic figure) for thresholding.
    amount = max(amounts) * mult
    label = f"≈₦{amount:,.0f}" if currency == "NGN" else f"{currency} ≈₦{amount:,.0f}"
    return amount, currency, label


# ─────────────────────────────────────────────────────────────────────────────
# CORE SCORER
# ─────────────────────────────────────────────────────────────────────────────
def score_signal(doc: dict[str, Any]) -> tuple[int, dict[str, Any]]:
    """
    Compute a 0-100 match score for a single signal document.

    Parameters
    ----------
    doc : dict
        A MongoDB signal document (or any dict with the same fields).
        Fields used:
          location, roleMode, roleType, applicationStatus,
          extractionConfidence, sourceConfidence,
          addedAt, postedAt,
          role, aiSummary, originalSourceText, skillTags

    Returns
    -------
    (score, breakdown) where:
      score     — integer 0-100
      breakdown — dict explaining every component score for debugging
    """
    breakdown: dict[str, Any] = {}

    # ── 1. SENIORITY (28 pts) ────────────────────────────────────────────────
    # The single most important question: is this entry-level or senior?
    # We use both keyword matching AND Gemini's extractionConfidence, which the
    # extraction prompt defines as: High = intern/junior/entry-level,
    # Medium = mid/senior, Low = unclear. So "High" is a direct entry-level vote.
    search_text = " ".join([
        (doc.get("role")               or ""),
        (doc.get("aiSummary")          or ""),
        (doc.get("originalSourceText") or ""),
    ]).lower()

    has_junior = any(kw in search_text for kw in SENIORITY_KEYWORDS)
    has_senior = any(kw in search_text for kw in SENIOR_KEYWORDS)

    # Accept either the DB field name or the raw-item name so this works whether
    # we're scoring a stored doc or a freshly-extracted item.
    conf = (doc.get("extractionConfidence") or doc.get("confidence") or "").strip().lower()
    gemini_entry = conf == "high"

    if has_junior and not has_senior:
        seniority_score = 28
        seniority_label = "Intern/Junior confirmed"
    elif gemini_entry and not has_senior:
        seniority_score = 25
        seniority_label = "Entry-level (AI-flagged)"
    elif has_junior and has_senior:
        seniority_score = 16
        seniority_label = "Mixed seniority signals"
    elif has_senior and gemini_entry:
        # Word "senior" appears but Gemini still read it as entry-level
        seniority_score = 14
        seniority_label = "Conflicting seniority signals"
    elif has_senior:
        seniority_score = 3
        seniority_label = "Senior-only keywords"
    else:
        seniority_score = 12
        seniority_label = "No seniority keywords"

    breakdown["seniority"] = {"score": seniority_score, "max": 28, "label": seniority_label}

    # ── 2. LOCATION (18 pts) ──────────────────────────────────────────────────
    # Nigeria is a bonus, but ANY remote role is welcome — "others are ok".
    loc = (doc.get("location") or "").strip()
    role_mode = (doc.get("roleMode") or "").strip()

    loc_lower = loc.lower()
    mode_lower = role_mode.lower()

    is_remote = "remote" in loc_lower or "remote" in mode_lower

    if "nigeria" in loc_lower and is_remote:
        loc_score = 18
        loc_label = "Nigeria Remote"
    elif is_remote:
        # Remote anywhere is great even without a Nigeria mention.
        loc_score = 15
        loc_label = "Remote (non-Nigeria)"
    elif "nigeria" in loc_lower and "onsite" in loc_lower.replace("-", "").replace(" ", ""):
        loc_score = 12
        loc_label = "Nigeria Onsite"
    elif "nigeria" in loc_lower:
        loc_score = 11
        loc_label = "Nigeria (mode unclear)"
    elif loc in ("", "Unknown"):
        loc_score = 6
        loc_label = "Unknown"
    else:
        loc_score = 2
        loc_label = f"Other ({loc})"

    breakdown["location"] = {"score": loc_score, "max": 18, "label": loc_label}

    # ── 3. PAY FIT (14 pts) ───────────────────────────────────────────────────
    # Intern stipend / modest NGN pay scores well; a "professional" ₦1m+ NGN
    # salary is a signal this is NOT an entry-level role and scores ~0.
    # Foreign-currency figures are ambiguous (monthly vs annual, different market
    # rates) so we stay neutral on them rather than over-penalising remote roles.
    pay_amount, pay_currency, pay_label = parse_pay(doc.get("pay"))
    if pay_amount is None:
        pay_score = 9          # not stated / unparseable — stay neutral
    elif pay_amount == 0.0:
        pay_score = 15         # explicit stipend / unpaid intern language
    elif pay_currency != "NGN":
        pay_score = 9          # foreign currency — too ambiguous to judge, neutral
    elif pay_amount < 100_000:
        pay_score = 14
    elif pay_amount < 300_000:
        pay_score = 11
    elif pay_amount < 600_000:
        pay_score = 6
    elif pay_amount < SENIOR_PAY_THRESHOLD:
        pay_score = 2
    else:
        pay_score = 0          # ₦1m+ — professional pay, the user's anti-match
    breakdown["pay_fit"] = {"score": pay_score, "max": 14, "label": pay_label}

    # ── 4. FRESHNESS (13 pts) ─────────────────────────────────────────────────
    # Prefer addedAt — it's when WE ingested the signal and what the user can act
    # on. postedAt (source publication date) is the fallback, and a missing date
    # is treated as neutral rather than zero so good-but-undated jobs aren't sunk.
    now = datetime.now(timezone.utc)
    age_days: Optional[float] = None

    ts = (doc.get("addedAt") or doc.get("postedAt") or "").strip()
    if ts:
        try:
            dt = datetime.fromisoformat(ts)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            age_days = max(0.0, (now - dt).total_seconds() / 86400)
        except Exception:
            age_days = None

    if age_days is None:
        freshness_score = 8
        freshness_label = "date unknown — neutral"
    elif age_days <= 1:
        freshness_score = 13
        freshness_label = f"{age_days:.1f}d — just added"
    elif age_days <= 4:
        freshness_score = round(12 - (age_days - 1) * (2 / 3))   # 12→10
        freshness_label = f"{age_days:.1f}d — very fresh"
    elif age_days <= 7:
        freshness_score = round(9 - (age_days - 4))              # 9→6
        freshness_label = f"{age_days:.1f}d — getting stale"
    elif age_days <= 14:
        freshness_score = round(5 - (age_days - 7) * (5 / 7))    # 5→0
        freshness_label = f"{age_days:.1f}d — old"
    else:
        freshness_score = 0
        freshness_label = f"{age_days:.1f}d — too old"

    freshness_score = max(0, min(13, freshness_score))
    breakdown["freshness"] = {"score": freshness_score, "max": 13, "label": freshness_label}

    # ── 5. ROLE MODE (12 pts) ─────────────────────────────────────────────────
    mode_map = {"remote": 12, "hybrid": 7, "on-site": 2, "onsite": 2}
    role_mode_score = mode_map.get(mode_lower, 5)
    breakdown["role_mode"] = {"score": role_mode_score, "max": 12, "label": role_mode or "Unknown"}

    # ── 6. ROLE TYPE (5 pts) ─────────────────────────────────────────────────
    rt = (doc.get("roleType") or "Software Engineering").strip()
    role_type_map = {
        "Software Engineering": 5,
        "Mobile":               5,
        "Data":                 4,
        "DevOps":               3,
        "QA":                   3,
        "Design":               2,
        "Other":                1,
    }
    role_type_score = role_type_map.get(rt, 3)
    breakdown["role_type"] = {"score": role_type_score, "max": 5, "label": rt}

    # ── 7. APPLICATION STATUS (4 pts) ────────────────────────────────────────
    status = (doc.get("applicationStatus") or "Unknown").strip()
    status_map = {
        "Open":         4,
        "Unknown":      2,
        "Closing soon": 3,  # still valid — just urgent
    }
    status_score = status_map.get(status, 2)
    breakdown["status"] = {"score": status_score, "max": 4, "label": status}

    # ── 8. RICHNESS / DATA QUALITY (3 pts) ───────────────────────────────────
    tags = doc.get("skillTags") or []
    n_tags = len([t for t in tags if t and t.strip()])
    if n_tags >= 5:
        richness_score = 3
    elif n_tags >= 3:
        richness_score = 2
    elif n_tags >= 1:
        richness_score = 1
    else:
        richness_score = 0
    breakdown["richness"] = {"score": richness_score, "max": 3, "label": f"{n_tags} tags"}

    # ── 9. SOURCE CONFIDENCE (3 pts) ─────────────────────────────────────────
    sc = (doc.get("sourceConfidence") or "Medium").strip()
    source_map = {"High": 3, "Medium": 2, "Low": 0}
    source_score = source_map.get(sc, 2)
    breakdown["source"] = {"score": source_score, "max": 3, "label": sc}

    # ── WEIGHTED BASE ──────────────────────────────────────────────────────────
    total = (
        seniority_score
        + loc_score
        + pay_score
        + freshness_score
        + role_mode_score
        + role_type_score
        + status_score
        + richness_score
        + source_score
    )

    # ── LEVEL PENALTY ──────────────────────────────────────────────────────────
    # A clearly senior / "professional" role (senior keywords, or ₦1m+ pay with
    # no junior signal) is the user's explicit anti-match. The weighted base alone
    # can't sink it — a senior role can still be remote, Nigerian, and fresh — so
    # we subtract a flat penalty AND hard-cap the ₦1m+ case to force it below 50.
    is_senior_pay = (
        pay_amount is not None
        and pay_currency == "NGN"
        and pay_amount >= SENIOR_PAY_THRESHOLD
        and not has_junior
        and not gemini_entry
    )
    is_senior = (has_senior and not has_junior and not gemini_entry) or is_senior_pay

    if is_senior:
        penalty = 30
        total -= penalty
        reason = "senior keywords" if (has_senior and not is_senior_pay) else "professional pay"
        breakdown["level_penalty"] = {"score": -penalty, "max": 0, "label": f"senior role ({reason})"}
        if is_senior_pay:
            total = min(total, 35)   # ₦1m+ never reads as a match

    total = max(0, min(100, total))

    breakdown["TOTAL"] = total
    return total, breakdown


# ─────────────────────────────────────────────────────────────────────────────
# BATCH DATABASE UPDATER
# ─────────────────────────────────────────────────────────────────────────────
async def rescore_all(dry_run: bool = False, verbose: bool = True) -> dict[str, int]:
    """
    Fetch every signal from MongoDB and overwrite its aiMatchScore with the
    new computed value.

    Parameters
    ----------
    dry_run : bool
        If True, prints what would change but writes nothing to the DB.
    verbose : bool
        Print per-signal details.

    Returns
    -------
    dict with keys: total, updated, unchanged, errors
    """
    client = AsyncIOMotorClient(MONGODB_URI)
    col    = client[DB_NAME]["signals"]

    stats = {"total": 0, "updated": 0, "unchanged": 0, "errors": 0}

    print(f"\n{'[DRY RUN] ' if dry_run else ''}🔁 Rescoring all signals in '{DB_NAME}.signals'…\n")

    cursor = col.find({})

    async for doc in cursor:
        stats["total"] += 1
        old_score = doc.get("aiMatchScore", -1)

        try:
            new_score, breakdown = score_signal(doc)
        except Exception as e:
            print(f"  ⚠  Error scoring {doc.get('_id')}: {e}")
            stats["errors"] += 1
            continue

        delta = new_score - old_score
        changed = (new_score != old_score)

        if verbose or changed:
            role    = doc.get("role",     "?")[:40]
            company = doc.get("company",  "?")[:30]
            loc     = doc.get("location", "?")[:25]
            arrow   = f"{old_score:>3} → {new_score:>3}" if changed else f"   {new_score:>3} (no change)"
            delta_s = f"  ({delta:+d})" if changed else ""
            print(f"  {arrow}{delta_s:>7}  |  {role} @ {company}  [{loc}]")

            if verbose and changed:
                for k, v in breakdown.items():
                    if k == "TOTAL":
                        continue
                    print(f"           {k:<12} {v['score']}/{v['max']}  {v['label']}")
                print()

        if changed:
            stats["updated"] += 1
            if not dry_run:
                try:
                    await col.update_one(
                        {"_id": doc["_id"]},
                        {"$set": {"aiMatchScore": new_score}},
                    )
                except Exception as e:
                    print(f"  ⚠  DB write error for {doc['_id']}: {e}")
                    stats["errors"] += 1
        else:
            stats["unchanged"] += 1

    client.close()

    print(f"\n{'[DRY RUN] — no writes performed' if dry_run else '✅ Done'}")
    print(f"   Total:      {stats['total']}")
    print(f"   Updated:    {stats['updated']}")
    print(f"   Unchanged:  {stats['unchanged']}")
    print(f"   Errors:     {stats['errors']}\n")
    return stats


# ─────────────────────────────────────────────────────────────────────────────
# CLI ENTRYPOINT
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Rescore all signals in the Jobless DB")
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print what would change but write nothing to MongoDB"
    )
    parser.add_argument(
        "--quiet", action="store_true",
        help="Only print changed signals, skip unchanged ones"
    )
    parser.add_argument(
        "--demo", action="store_true",
        help="Score a handful of synthetic examples to verify the function works"
    )
    args = parser.parse_args()

    if args.demo:
        # ── Quick sanity-check without needing MongoDB ────────────────────
        now_iso  = datetime.now(timezone.utc).isoformat()
        days_ago = lambda n: (datetime.now(timezone.utc) - timedelta(days=n)).isoformat()
        examples = [
            {
                "name": "🏆 Perfect: Nigeria Remote Intern, stipend, 1 day old",
                "doc": {
                    "location": "Nigeria Remote", "roleMode": "Remote",
                    "roleType": "Software Engineering", "applicationStatus": "Open",
                    "addedAt": now_iso, "pay": "₦80,000 monthly stipend",
                    "role": "Frontend Intern", "aiSummary": "Junior internship for fresh grads",
                    "originalSourceText": "We are hiring an intern to join our team",
                    "skillTags": ["React", "TypeScript", "CSS", "Git", "Node.js"],
                    "extractionConfidence": "High", "sourceConfidence": "High",
                },
            },
            {
                "name": "🥈 OK (others welcome): Remote non-Nigeria Junior, 3 days old",
                "doc": {
                    "location": "Remote", "roleMode": "Remote",
                    "roleType": "Software Engineering", "applicationStatus": "Open",
                    "addedAt": days_ago(3), "pay": "$1,200/month",
                    "role": "Junior React Developer",
                    "aiSummary": "Entry-level position for new grads",
                    "originalSourceText": "We are hiring entry level developers",
                    "skillTags": ["React", "JavaScript"],
                    "extractionConfidence": "High", "sourceConfidence": "Medium",
                },
            },
            {
                "name": "🚫 Anti-match: Nigeria Remote 'professional' ₦1.5m, fresh",
                "doc": {
                    "location": "Nigeria Remote", "roleMode": "Remote",
                    "roleType": "Software Engineering", "applicationStatus": "Open",
                    "addedAt": now_iso, "pay": "₦1,500,000 per month",
                    "role": "Backend Engineer",
                    "aiSummary": "Experienced engineer for a fast-growing fintech",
                    "originalSourceText": "We are hiring an experienced backend engineer",
                    "skillTags": ["Python", "Django", "PostgreSQL"],
                    "extractionConfidence": "Medium", "sourceConfidence": "High",
                },
            },
            {
                "name": "💀 Bad: USA Onsite Senior, 20 days old",
                "doc": {
                    "location": "San Francisco, CA", "roleMode": "On-site",
                    "roleType": "Software Engineering", "applicationStatus": "Closing soon",
                    "addedAt": days_ago(20), "pay": "$180,000 / year",
                    "role": "Principal Engineer",
                    "aiSummary": "Lead engineer needed",
                    "originalSourceText": "Seeking a principal engineer with 10+ years",
                    "skillTags": ["Go", "Kubernetes"],
                    "extractionConfidence": "Medium", "sourceConfidence": "Low",
                },
            },
        ]

        print("\n" + "═" * 60)
        print("  DEMO MODE — scoring synthetic examples")
        print("═" * 60 + "\n")

        for ex in examples:
            score, bd = score_signal(ex["doc"])
            print(f"{ex['name']}")
            print(f"  → Score: {score}/100")
            for k, v in bd.items():
                if k == "TOTAL":
                    continue
                bar = "█" * v["score"] + "░" * (v["max"] - v["score"])
                print(f"     {k:<12} {bar}  {v['score']}/{v['max']}  {v['label']}")
            print()

        sys.exit(0)

    if not MONGODB_URI:
        print("❌  MONGODB_URI not set. Check your .env file.")
        sys.exit(1)

    asyncio.run(rescore_all(dry_run=args.dry_run, verbose=not args.quiet))