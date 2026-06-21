import json

import pytest
from pydantic import ValidationError

from main import GeminiListingResult, _parse_gemini_response, build_scraped_id, item_to_doc


# ── build_scraped_id ──────────────────────────────────────────────────────────

def test_build_scraped_id_combines_source_and_id():
    assert build_scraped_id({"source": "Remotive", "id": "123"}) == "Remotive::123"


# ── item_to_doc ───────────────────────────────────────────────────────────────

def make_raw_item(**overrides) -> dict:
    base = {
        "id": "abc123",
        "source": "Remotive",
        "text": "Frontend Intern at Acme\nLocation: Remote\nURL: https://example.com/job",
        "created_at": "2026-06-01T00:00:00+00:00",
        "url": "https://example.com/job",
        "user": "Acme",
        "username": "acme",
        "company": "Acme",
        "position": "Frontend Intern",
        "pay": "₦80,000 stipend",
        "location": "Nigeria Remote",
        "role_mode": "Remote",
        "role_type": "Software Engineering",
        "application_status": "Open",
        "skill_tags": ["React", "TypeScript"],
        "skill_alignment": "React, TypeScript",
        "relevance_reason": "Entry-level frontend role",
        "notes": "Frontend internship for fresh grads",
        "apply_link": "https://example.com/apply",
        "confidence": "High",
        "source_confidence": "High",
    }
    base.update(overrides)
    return base


def test_item_to_doc_maps_gemini_fields_to_db_schema():
    doc = item_to_doc(make_raw_item())
    assert doc["scrapedId"] == "Remotive::abc123"
    assert doc["role"] == "Frontend Intern"
    assert doc["company"] == "Acme"
    assert doc["location"] == "Nigeria Remote"
    assert doc["roleMode"] == "Remote"
    assert doc["applicationStatus"] == "Open"
    assert doc["extractionConfidence"] == "High"
    assert doc["skillTags"] == ["React", "TypeScript"]
    assert isinstance(doc["aiMatchScore"], int)
    assert 0 <= doc["aiMatchScore"] <= 100


def test_item_to_doc_normalises_role_mode_case():
    doc = item_to_doc(make_raw_item(role_mode="onsite"))
    assert doc["roleMode"] == "On-site"


def test_item_to_doc_falls_back_on_missing_gemini_fields():
    raw = {
        "id": "xyz",
        "source": "Telegram",
        "text": "Some raw message text",
        "url": "",
        "username": "channel",
    }
    doc = item_to_doc(raw)
    assert doc["role"] == "Unknown"
    assert doc["company"] == "Unknown"
    assert doc["roleMode"] == "Remote"          # default when unspecified
    assert doc["applicationStatus"] == "Unknown"
    assert doc["pay"] is None
    assert doc["aiSummary"]                      # falls back to truncated raw text


# ── GeminiListingResult / _parse_gemini_response ──────────────────────────────

def test_gemini_listing_result_requires_index_and_is_valid():
    with pytest.raises(ValidationError):
        GeminiListingResult.model_validate({"company": "Acme"})


def test_parse_gemini_response_keeps_valid_items_and_merges_batch():
    batch = [make_raw_item(id="a"), make_raw_item(id="b", position="Backend Intern")]
    raw = json.dumps([
        {"index": 0, "is_valid": True, "position": "Frontend Intern", "confidence": "High"},
        {"index": 1, "is_valid": False, "reason": "not a tech role"},
    ])
    results = _parse_gemini_response(raw, batch)
    assert len(results) == 1
    assert results[0]["position"] == "Frontend Intern"
    assert results[0]["id"] == "a"   # original raw fields are preserved/merged


def test_parse_gemini_response_drops_out_of_range_index():
    batch = [make_raw_item(id="a")]
    raw = json.dumps([{"index": 5, "is_valid": True}])
    assert _parse_gemini_response(raw, batch) == []


def test_parse_gemini_response_strips_markdown_fences_defensively():
    batch = [make_raw_item(id="a")]
    raw = "```json\n" + json.dumps([{"index": 0, "is_valid": True}]) + "\n```"
    results = _parse_gemini_response(raw, batch)
    assert len(results) == 1


def test_parse_gemini_response_raises_on_malformed_json():
    with pytest.raises(json.JSONDecodeError):
        _parse_gemini_response("not json at all", [make_raw_item()])


def test_parse_gemini_response_raises_on_invalid_shape():
    batch = [make_raw_item()]
    raw = json.dumps([{"is_valid": "not-a-bool-and-no-index"}])
    with pytest.raises(ValidationError):
        _parse_gemini_response(raw, batch)
