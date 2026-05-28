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
import sys
from datetime import datetime, timezone
from typing import Any

from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient

load_dotenv()

MONGODB_URI = os.getenv("MONGODB_URI", "")
DB_NAME     = os.getenv("DB_NAME", "jobless")

# ─────────────────────────────────────────────────────────────────────────────
# SCORING WEIGHTS  (must sum to 100)
# ─────────────────────────────────────────────────────────────────────────────
#
# What makes a "perfect" signal for this user:
#   ✅  Location  →  Nigeria Remote (best) or Nigeria Onsite (ok) > Remote > Other
#   ✅  Seniority →  Intern / Junior / Entry-level keywords are gold
#   ✅  Freshness →  ≤ 4 days old = full points; degrades fast after that
#   ✅  Role mode →  Remote > Hybrid > On-site
#   ✅  Role type →  Software Engineering / Mobile preferred
#   ✅  Status    →  Open > Unknown >> Closing soon (ironically still valuable)
#   ✅  Richness  →  more skill tags = more signal that Gemini got good data
#   ✅  Source    →  High confidence sources are more trustworthy
#
WEIGHTS = {
    "location":   30,   # single biggest factor — Nigerian remote is the goal
    "seniority":  25,   # intern/junior keyword presence
    "freshness":  20,   # age of the listing (capped at 14 days, steep curve)
    "role_mode":  10,   # remote > hybrid > on-site
    "role_type":   5,   # software / mobile preferred
    "status":      5,   # open > unknown > closing
    "richness":    3,   # skill tag count — proxy for data quality
    "source":      2,   # source confidence
}
assert sum(WEIGHTS.values()) == 100, "Weights must sum to 100"

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

    # ── 1. LOCATION (30 pts) ──────────────────────────────────────────────────
    loc = (doc.get("location") or "").strip()
    role_mode = (doc.get("roleMode") or "").strip()

    loc_lower = loc.lower()
    mode_lower = role_mode.lower()

    # Nigeria Remote is the jackpot
    if "nigeria" in loc_lower and ("remote" in loc_lower or "remote" in mode_lower):
        loc_score = 30
        loc_label = "Nigeria Remote"
    elif "nigeria" in loc_lower and "onsite" in loc_lower.replace("-", "").replace(" ", ""):
        loc_score = 20
        loc_label = "Nigeria Onsite"
    elif "nigeria" in loc_lower:
        loc_score = 18
        loc_label = "Nigeria (mode unclear)"
    elif loc == "Remote" or "remote" in loc_lower:
        loc_score = 14
        loc_label = "Remote (non-Nigeria)"
    elif loc in ("", "Unknown"):
        loc_score = 5
        loc_label = "Unknown"
    else:
        loc_score = 2
        loc_label = f"Other ({loc})"

    breakdown["location"] = {"score": loc_score, "max": 30, "label": loc_label}

    # ── 2. SENIORITY (25 pts) ────────────────────────────────────────────────
    # Search role title, summary, and raw text
    search_text = " ".join([
        (doc.get("role")               or ""),
        (doc.get("aiSummary")          or ""),
        (doc.get("originalSourceText") or ""),
    ]).lower()

    has_junior = any(kw in search_text for kw in SENIORITY_KEYWORDS)
    has_senior = any(kw in search_text for kw in SENIOR_KEYWORDS)

    if has_junior and not has_senior:
        seniority_score = 25
        seniority_label = "Intern/Junior confirmed"
    elif has_junior and has_senior:
        # Both keywords — probably a team posting with mixed levels
        seniority_score = 15
        seniority_label = "Mixed seniority signals"
    elif not has_junior and not has_senior:
        # No seniority keywords at all — could still be entry level, penalise lightly
        seniority_score = 10
        seniority_label = "No seniority keywords"
    else:
        # Senior only — still a tech job, just not ideal
        seniority_score = 3
        seniority_label = "Senior-only keywords"

    breakdown["seniority"] = {"score": seniority_score, "max": 25, "label": seniority_label}

    # ── 3. FRESHNESS (20 pts) ────────────────────────────────────────────────
    # ONLY use addedAt — it's when WE ingested the signal, always reliable.
    # postedAt is the source publication date: often months old, sometimes
    # empty, and has nothing to do with when the user can act on the listing.
    now = datetime.now(timezone.utc)
    age_days: float = 999.0

    ts = (doc.get("postedAt") or "").strip()
    if ts:
        try:
            dt = datetime.fromisoformat(ts)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            age_days = (now - dt).total_seconds() / 86400
            # Sanity-check: future timestamps mean clock skew — treat as 0 days
            age_days = max(0.0, age_days)
        except Exception:
            pass  # age_days stays 999 → freshness_score = 0

    # Score curve:
    #   0-1 days  → 20 pts  (just landed)
    #   1-4 days  → 15-19   (still very fresh — user's sweet spot)
    #   4-7 days  → 8-14    (getting stale)
    #   7-14 days → 2-7     (old but still exists)
    #   > 14 days → 0       (ancient)
    if age_days <= 1:
        freshness_score = 20
        freshness_label = f"{age_days:.1f}d — just added"
    elif age_days <= 4:
        # Linear 15→19 across 1-4 days
        freshness_score = round(19 - (age_days - 1) * (4 / 3))
        freshness_label = f"{age_days:.1f}d — very fresh"
    elif age_days <= 7:
        freshness_score = round(14 - (age_days - 4) * 2)
        freshness_label = f"{age_days:.1f}d — getting stale"
    elif age_days <= 14:
        freshness_score = round(8 - (age_days - 7) * (6 / 7))
        freshness_label = f"{age_days:.1f}d — old"
    else:
        freshness_score = 0
        freshness_label = f"{age_days:.1f}d — too old"

    freshness_score = max(0, min(20, freshness_score))
    breakdown["freshness"] = {"score": freshness_score, "max": 20, "label": freshness_label}

    # ── 4. ROLE MODE (10 pts) ────────────────────────────────────────────────
    mode_map = {"remote": 10, "hybrid": 6, "on-site": 2, "onsite": 2}
    role_mode_score = mode_map.get(mode_lower, 4)
    breakdown["role_mode"] = {"score": role_mode_score, "max": 10, "label": role_mode or "Unknown"}

    # ── 5. ROLE TYPE (5 pts) ─────────────────────────────────────────────────
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

    # ── 6. APPLICATION STATUS (5 pts) ────────────────────────────────────────
    status = (doc.get("applicationStatus") or "Unknown").strip()
    status_map = {
        "Open":         5,
        "Unknown":      3,
        "Closing soon": 4,  # still valid — just urgent
    }
    status_score = status_map.get(status, 3)
    breakdown["status"] = {"score": status_score, "max": 5, "label": status}

    # ── 7. RICHNESS / DATA QUALITY (3 pts) ───────────────────────────────────
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

    # ── 8. SOURCE CONFIDENCE (2 pts) ─────────────────────────────────────────
    sc = (doc.get("sourceConfidence") or "Medium").strip()
    source_map = {"High": 2, "Medium": 1, "Low": 0}
    source_score = source_map.get(sc, 1)
    breakdown["source"] = {"score": source_score, "max": 2, "label": sc}

    # ── TOTAL ─────────────────────────────────────────────────────────────────
    total = (
        loc_score
        + seniority_score
        + freshness_score
        + role_mode_score
        + role_type_score
        + status_score
        + richness_score
        + source_score
    )
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
        examples = [
            {
                "name": "🏆 Perfect: Nigeria Remote Intern, 1 day old",
                "doc": {
                    "location": "Nigeria Remote", "roleMode": "Remote",
                    "roleType": "Software Engineering", "applicationStatus": "Open",
                    "addedAt": datetime.now(timezone.utc).isoformat(),
                    "role": "Frontend Intern", "aiSummary": "Junior internship for fresh grads",
                    "originalSourceText": "We are hiring an intern to join our team",
                    "skillTags": ["React", "TypeScript", "CSS", "Git", "Node.js"],
                    "sourceConfidence": "High",
                },
            },
            {
                "name": "🥈 Good: Nigeria Remote but senior, 2 days old",
                "doc": {
                    "location": "Nigeria Remote", "roleMode": "Remote",
                    "roleType": "Software Engineering", "applicationStatus": "Open",
                    "addedAt": (datetime.now(timezone.utc).replace(
                        hour=datetime.now().hour - 48 % 24
                    )).isoformat(),
                    "role": "Senior Backend Engineer",
                    "aiSummary": "Senior role, 5+ years required",
                    "originalSourceText": "Senior developer needed with 7 years experience",
                    "skillTags": ["Python", "Django", "PostgreSQL"],
                    "sourceConfidence": "High",
                },
            },
            {
                "name": "🥉 OK: Remote (non-Nigeria) Junior, 3 days old",
                "doc": {
                    "location": "Remote", "roleMode": "Remote",
                    "roleType": "Software Engineering", "applicationStatus": "Open",
                    "addedAt": (datetime.now(timezone.utc).isoformat()),
                    "role": "Junior React Developer",
                    "aiSummary": "Entry-level position for new grads",
                    "originalSourceText": "We are hiring entry level developers",
                    "skillTags": ["React", "JavaScript"],
                    "sourceConfidence": "Medium",
                },
            },
            {
                "name": "💀 Bad: USA Onsite Senior, 20 days old",
                "doc": {
                    "location": "San Francisco, CA", "roleMode": "On-site",
                    "roleType": "Software Engineering", "applicationStatus": "Closing soon",
                    "addedAt": "2025-05-01T00:00:00+00:00",
                    "role": "Principal Engineer",
                    "aiSummary": "Lead engineer needed",
                    "originalSourceText": "Seeking a principal engineer with 10+ years",
                    "skillTags": ["Go", "Kubernetes"],
                    "sourceConfidence": "Low",
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