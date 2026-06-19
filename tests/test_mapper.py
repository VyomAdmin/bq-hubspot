import pytest
from sync.mapper import build_hubspot_properties, build_batch_payload

MAPPINGS = {
    "contacts": {
        "bq_email": "email",
        "bq_mrr":   "hs_mrr__c",
        "bq_first": "firstname",
    },
    "companies": {
        "bq_domain": "domain",
    },
}


# ---------------------------------------------------------------------------
# build_hubspot_properties
# ---------------------------------------------------------------------------

def test_maps_known_columns():
    result = build_hubspot_properties(
        {"bq_email": "foo@bar.com", "bq_mrr": 1200},
        "contacts",
        MAPPINGS,
    )
    assert result == {"email": "foo@bar.com", "hs_mrr__c": 1200}


def test_skips_unmapped_columns():
    result = build_hubspot_properties(
        {"bq_email": "a@b.com", "bq_unknown": "x"},
        "contacts",
        MAPPINGS,
    )
    assert "bq_unknown" not in result
    assert result == {"email": "a@b.com"}


def test_skips_null_values():
    result = build_hubspot_properties(
        {"bq_email": None, "bq_mrr": 500},
        "contacts",
        MAPPINGS,
    )
    assert "email" not in result
    assert result["hs_mrr__c"] == 500


def test_numeric_values_passed_as_is():
    result = build_hubspot_properties({"bq_mrr": 9999}, "contacts", MAPPINGS)
    assert result["hs_mrr__c"] == 9999
    assert isinstance(result["hs_mrr__c"], int)


def test_string_values_coerced_to_string():
    # Non-numeric, non-bool values are coerced to str
    result = build_hubspot_properties({"bq_email": object()}, "contacts", MAPPINGS)
    assert isinstance(result["email"], str)


def test_unknown_object_type_returns_empty():
    result = build_hubspot_properties({"bq_email": "a@b.com"}, "deals", MAPPINGS)
    assert result == {}


# ---------------------------------------------------------------------------
# build_batch_payload
# ---------------------------------------------------------------------------

_DEFAULT_PROPS = {"bq_email": "x@y.com"}

def _make_row(row_id, hubspot_id=None, bk=None, bk_prop=None, props=_DEFAULT_PROPS):
    return {
        "row_id": row_id,
        "hubspot_id": hubspot_id,
        "business_key": bk,
        "business_key_property": bk_prop,
        "properties": props,
    }


def test_id_based_row():
    rows = [_make_row("r1", hubspot_id="123")]
    result = build_batch_payload(rows, "contacts", MAPPINGS)
    assert len(result) == 1
    assert result[0]["payload"]["id"] == "123"
    assert "idProperty" not in result[0]["payload"]
    assert result[0]["row_id"] == "r1"
    assert result[0]["hs_id"] == "123"


def test_property_based_row():
    rows = [_make_row("r2", bk="x@y.com", bk_prop="email")]
    result = build_batch_payload(rows, "contacts", MAPPINGS)
    assert result[0]["payload"]["id"] == "x@y.com"
    assert result[0]["payload"]["idProperty"] == "email"


def test_row_with_no_identifier_skipped():
    rows = [_make_row("r3")]  # no hubspot_id, no business_key
    result = build_batch_payload(rows, "contacts", MAPPINGS)
    assert result == []


def test_row_with_empty_properties_skipped():
    rows = [_make_row("r4", hubspot_id="999", props={})]
    result = build_batch_payload(rows, "contacts", MAPPINGS)
    assert result == []


def test_multiple_rows_ordered():
    rows = [
        _make_row("r1", hubspot_id="1"),
        _make_row("r2", hubspot_id="2"),
        _make_row("r3", hubspot_id="3"),
    ]
    result = build_batch_payload(rows, "contacts", MAPPINGS)
    assert [r["row_id"] for r in result] == ["r1", "r2", "r3"]
