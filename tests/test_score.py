from datetime import datetime, timedelta, timezone

import pytest

from score import parse_pay, score_signal


def days_ago(n: float) -> str:
    return (datetime.now(timezone.utc) - timedelta(days=n)).isoformat()


# ── parse_pay ────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("raw,expected_amount,expected_currency", [
    ("₦1,000,000", 1_000_000, "NGN"),
    ("1m", 1_000_000, "NGN"),
    ("500k/month", 500_000, "NGN"),
    ("$2000", 2000 * 1500.0, "USD"),
    ("₦250,000 - ₦400,000", 400_000, "NGN"),
])
def test_parse_pay_amounts(raw, expected_amount, expected_currency):
    amount, currency, _label = parse_pay(raw)
    assert amount == expected_amount
    assert currency == expected_currency


@pytest.mark.parametrize("raw", ["Unpaid internship", "stipend", "volunteer"])
def test_parse_pay_unpaid_is_zero(raw):
    amount, currency, label = parse_pay(raw)
    assert amount == 0.0
    assert "stipend" in label or "unpaid" in label


@pytest.mark.parametrize("raw", [None, "", "   ", "Competitive"])
def test_parse_pay_missing_or_unparseable(raw):
    amount, _currency, _label = parse_pay(raw)
    assert amount is None


# ── score_signal ─────────────────────────────────────────────────────────────

def make_doc(**overrides) -> dict:
    base = {
        "location":             "Nigeria Remote",
        "roleMode":              "Remote",
        "roleType":              "Software Engineering",
        "applicationStatus":     "Open",
        "addedAt":               days_ago(0),
        "pay":                   "₦80,000 monthly stipend",
        "role":                  "Frontend Intern",
        "aiSummary":             "Junior internship for fresh grads",
        "originalSourceText":    "We are hiring an intern to join our team",
        "skillTags":             ["React", "TypeScript", "CSS", "Git", "Node.js"],
        "extractionConfidence":  "High",
        "sourceConfidence":      "High",
    }
    base.update(overrides)
    return base


def test_perfect_match_scores_high():
    score, breakdown = score_signal(make_doc())
    assert score >= 85
    assert breakdown["seniority"]["score"] == 28


def test_senior_pay_anti_match_is_capped_low():
    doc = make_doc(
        pay="₦1,500,000 per month",
        role="Backend Engineer",
        aiSummary="Experienced engineer for a fast-growing fintech",
        originalSourceText="We are hiring an experienced backend engineer",
        extractionConfidence="Medium",
        skillTags=["Python", "Django", "PostgreSQL"],
    )
    score, breakdown = score_signal(doc)
    assert score <= 35
    assert "level_penalty" in breakdown


def test_senior_keywords_trigger_penalty():
    doc = make_doc(
        role="Principal Engineer",
        aiSummary="Lead engineer needed",
        originalSourceText="Seeking a principal engineer with 10+ years",
        location="San Francisco, CA",
        roleMode="On-site",
        addedAt=days_ago(20),
        extractionConfidence="Medium",
        sourceConfidence="Low",
        skillTags=["Go", "Kubernetes"],
    )
    score, breakdown = score_signal(doc)
    assert breakdown["level_penalty"]["score"] == -30
    assert score < 50


def test_missing_pay_and_date_are_neutral_not_zero():
    doc = make_doc(pay=None, addedAt=None)
    score, breakdown = score_signal(doc)
    assert breakdown["pay_fit"]["score"] > 0
    assert breakdown["freshness"]["score"] > 0
    assert score > 0


def test_score_is_clamped_between_0_and_100():
    score, _ = score_signal(make_doc())
    assert 0 <= score <= 100
