from unittest.mock import MagicMock, patch, call

import pytest

from sync.config import Config
from sync.runner import run_sync, _chunk


# ---------------------------------------------------------------------------
# _chunk utility
# ---------------------------------------------------------------------------

def test_chunk_even():
    assert list(_chunk([1, 2, 3, 4], 2)) == [[1, 2], [3, 4]]


def test_chunk_uneven():
    assert list(_chunk([1, 2, 3], 2)) == [[1, 2], [3]]


def test_chunk_empty():
    assert list(_chunk([], 10)) == []


def test_chunk_larger_than_list():
    assert list(_chunk([1, 2], 100)) == [[1, 2]]


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

MAPPINGS = {
    "contacts": {"bq_email": "email"},
    "companies": {"bq_domain": "domain"},
}

BASE_CFG = Config(
    project_id="proj",
    bq_dataset="ds",
    bq_table="tbl",
    hubspot_token="tok",
    window_minutes=10,
    batch_size=100,
    max_rows_per_run=5000,
    max_requests_per_minute=100,
    mappings=MAPPINGS,
)


def _pending_rows(n=2, object_type="contacts"):
    return [
        {
            "row_id": f"r{i}",
            "object_type": object_type,
            "hubspot_id": str(i),
            "business_key": None,
            "business_key_property": None,
            "properties": {"bq_email": f"user{i}@test.com"},
            "retry_count": 0,
        }
        for i in range(1, n + 1)
    ]


# ---------------------------------------------------------------------------
# run_sync — no-op when nothing pending
# ---------------------------------------------------------------------------

def test_run_sync_no_rows():
    with patch("sync.runner.BigQueryClient") as MockBQ, \
         patch("sync.runner.HubSpotClient"), \
         patch("sync.runner.make_retrying_batch_update"):
        bq_inst = MockBQ.return_value
        bq_inst.fetch_pending.return_value = []

        stats = run_sync(BASE_CFG)

    assert stats["fetched"] == 0
    assert stats["successes"] == 0
    bq_inst.mark_in_progress.assert_not_called()
    bq_inst.apply_results.assert_not_called()


# ---------------------------------------------------------------------------
# run_sync — happy path (all succeed)
# ---------------------------------------------------------------------------

def test_run_sync_all_succeed():
    rows = _pending_rows(3)
    hs_response = {
        "results": [{"id": "1"}, {"id": "2"}, {"id": "3"}],
        "errors": [],
    }

    with patch("sync.runner.BigQueryClient") as MockBQ, \
         patch("sync.runner.HubSpotClient"), \
         patch("sync.runner.make_retrying_batch_update") as mock_retry_fn:
        bq_inst = MockBQ.return_value
        bq_inst.fetch_pending.return_value = rows
        mock_retry_fn.return_value = MagicMock(return_value=hs_response)

        stats = run_sync(BASE_CFG)

    assert stats["fetched"] == 3
    assert stats["successes"] == 3
    assert stats["failures"] == 0

    successes, failures = bq_inst.apply_results.call_args[0]
    assert set(successes) == {"r1", "r2", "r3"}
    assert failures == []


# ---------------------------------------------------------------------------
# run_sync — partial failure (207)
# ---------------------------------------------------------------------------

def test_run_sync_partial_failure():
    rows = _pending_rows(3)
    hs_response = {
        "results": [{"id": "1"}, {"id": "2"}],
        "errors": [{"id": "3", "message": "property not found"}],
    }

    with patch("sync.runner.BigQueryClient") as MockBQ, \
         patch("sync.runner.HubSpotClient"), \
         patch("sync.runner.make_retrying_batch_update") as mock_retry_fn:
        bq_inst = MockBQ.return_value
        bq_inst.fetch_pending.return_value = rows
        mock_retry_fn.return_value = MagicMock(return_value=hs_response)

        stats = run_sync(BASE_CFG)

    assert stats["successes"] == 2
    assert stats["failures"] == 1


# ---------------------------------------------------------------------------
# run_sync — entire batch raises (network / 5xx after retries exhausted)
# ---------------------------------------------------------------------------

def test_run_sync_batch_exception_marks_all_failed():
    rows = _pending_rows(2)

    with patch("sync.runner.BigQueryClient") as MockBQ, \
         patch("sync.runner.HubSpotClient"), \
         patch("sync.runner.make_retrying_batch_update") as mock_retry_fn:
        bq_inst = MockBQ.return_value
        bq_inst.fetch_pending.return_value = rows
        mock_retry_fn.return_value = MagicMock(side_effect=Exception("HubSpot down"))

        stats = run_sync(BASE_CFG)

    assert stats["failures"] == 2
    assert stats["successes"] == 0

    _, failures = bq_inst.apply_results.call_args[0]
    assert all("HubSpot down" in msg for _, msg in failures)


# ---------------------------------------------------------------------------
# run_sync — mixed object types dispatched separately
# ---------------------------------------------------------------------------

def test_run_sync_groups_by_object_type():
    contact_rows = _pending_rows(2, object_type="contacts")
    company_rows = _pending_rows(1, object_type="companies")
    # Give companies different ids to avoid clash
    company_rows[0]["hubspot_id"] = "99"
    company_rows[0]["row_id"] = "rc1"
    company_rows[0]["properties"] = {"bq_domain": "acme.com"}

    all_rows = contact_rows + company_rows
    hs_success = {"results": [{"id": "1"}, {"id": "2"}, {"id": "99"}], "errors": []}

    calls_made = []

    def fake_update(object_type, inputs):
        calls_made.append(object_type)
        return hs_success

    with patch("sync.runner.BigQueryClient") as MockBQ, \
         patch("sync.runner.HubSpotClient"), \
         patch("sync.runner.make_retrying_batch_update") as mock_retry_fn:
        bq_inst = MockBQ.return_value
        bq_inst.fetch_pending.return_value = all_rows
        mock_retry_fn.return_value = fake_update

        run_sync(BASE_CFG)

    assert sorted(calls_made) == ["companies", "contacts"]
