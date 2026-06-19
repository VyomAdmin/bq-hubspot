from unittest.mock import MagicMock, patch, call
from datetime import timezone

import pytest

from sync.config import Config
from sync.invoice_runner import (
    _build_properties,
    _resolve_deal_ids,
    run_invoice_sync,
    TRANSITION_VO_WO,
    TRANSITION_WO_IN,
)

BASE_CFG = Config(
    project_id="proj",
    bq_dataset="ds",
    bq_table="tbl",
    hubspot_token="tok",
    invoice_project="psychic-lens-456414-e4",
    invoice_dataset="omega_stg",
    invoice_view="invoices_unique_view",
    invoice_state_table="invoice_sync_state",
    hs_deal_pipeline="691581097",
    hs_won_stage="1013210905",
    hs_omega_job_property="omega_job__c",
    hs_install_completed_property="status_code__c",
)


# ---------------------------------------------------------------------------
# _build_properties
# ---------------------------------------------------------------------------

def test_build_properties_vo_to_wo():
    props = _build_properties(TRANSITION_VO_WO, BASE_CFG)
    assert props["dealstage"] == "1013210905"
    assert props["pipeline"] == "691581097"


def test_build_properties_wo_to_in():
    props = _build_properties(TRANSITION_WO_IN, BASE_CFG)
    assert "status_code__c" in props
    # Should be a UTC ISO string
    val = props["status_code__c"]
    assert val.endswith("Z")
    assert "T" in val


def test_build_properties_unknown_transition_raises():
    with pytest.raises(ValueError, match="Unknown transition"):
        _build_properties("XX_TO_YY", BASE_CFG)


# ---------------------------------------------------------------------------
# _resolve_deal_ids
# ---------------------------------------------------------------------------

def test_resolve_deal_ids_skips_already_resolved():
    transitions = [
        {"invoice_id": "INV-1", "hubspot_deal_id": "deal-999", "transition": TRANSITION_VO_WO}
    ]
    mock_hs = MagicMock()
    result = _resolve_deal_ids(transitions, mock_hs, BASE_CFG)
    mock_hs.search_deals_by_property.assert_not_called()
    assert result[0]["hubspot_deal_id"] == "deal-999"


def test_resolve_deal_ids_calls_search_for_missing():
    transitions = [
        {"invoice_id": "INV-1", "hubspot_deal_id": None, "transition": TRANSITION_VO_WO},
        {"invoice_id": "INV-2", "hubspot_deal_id": None, "transition": TRANSITION_WO_IN},
    ]
    mock_hs = MagicMock()
    mock_hs.search_deals_by_property.return_value = {"INV-1": "d1", "INV-2": "d2"}

    result = _resolve_deal_ids(transitions, mock_hs, BASE_CFG)

    mock_hs.search_deals_by_property.assert_called_once_with(
        "omega_job__c", ["INV-1", "INV-2"]
    )
    assert result[0]["hubspot_deal_id"] == "d1"
    assert result[1]["hubspot_deal_id"] == "d2"


def test_resolve_deal_ids_missing_deal_stays_none():
    transitions = [{"invoice_id": "INV-X", "hubspot_deal_id": None, "transition": TRANSITION_VO_WO}]
    mock_hs = MagicMock()
    mock_hs.search_deals_by_property.return_value = {}  # not found in HubSpot

    result = _resolve_deal_ids(transitions, mock_hs, BASE_CFG)
    assert result[0]["hubspot_deal_id"] is None


# ---------------------------------------------------------------------------
# run_invoice_sync — orchestration
# ---------------------------------------------------------------------------

def _make_transition(invoice_id, transition, deal_id="deal-1"):
    return {
        "invoice_id": invoice_id,
        "previous_status": transition.split("_TO_")[0].replace("_", ""),
        "current_status": transition.split("_TO_")[1].replace("_", ""),
        "transition": transition,
        "hubspot_deal_id": deal_id,
    }


def test_run_invoice_sync_no_transitions():
    with patch("sync.invoice_runner.bigquery.Client"), \
         patch("sync.invoice_runner.HubSpotClient"), \
         patch("sync.invoice_runner.make_retrying_update_deal"), \
         patch("sync.invoice_runner._fetch_transitions", return_value=[]), \
         patch("sync.invoice_runner._update_state") as mock_update:

        stats = run_invoice_sync(BASE_CFG)

    assert stats["transitions_detected"] == 0
    assert stats["successes"] == 0
    mock_update.assert_not_called()


def test_run_invoice_sync_vo_to_wo_success():
    transition = _make_transition("INV-1", TRANSITION_VO_WO)
    mock_update_fn = MagicMock(return_value={"id": "deal-1"})

    with patch("sync.invoice_runner.bigquery.Client"), \
         patch("sync.invoice_runner.HubSpotClient"), \
         patch("sync.invoice_runner.make_retrying_update_deal", return_value=mock_update_fn), \
         patch("sync.invoice_runner._fetch_transitions", return_value=[transition]), \
         patch("sync.invoice_runner._resolve_deal_ids", return_value=[transition]), \
         patch("sync.invoice_runner._update_state") as mock_update:

        stats = run_invoice_sync(BASE_CFG)

    assert stats["vo_to_wo"] == 1
    assert stats["successes"] == 1
    assert stats["failures"] == 0

    # Verify the correct properties were sent
    call_args = mock_update_fn.call_args
    assert call_args[0][0] == "deal-1"
    assert call_args[0][1]["dealstage"] == "1013210905"


def test_run_invoice_sync_wo_to_in_success():
    transition = _make_transition("INV-2", TRANSITION_WO_IN, deal_id="deal-2")
    mock_update_fn = MagicMock(return_value={"id": "deal-2"})

    with patch("sync.invoice_runner.bigquery.Client"), \
         patch("sync.invoice_runner.HubSpotClient"), \
         patch("sync.invoice_runner.make_retrying_update_deal", return_value=mock_update_fn), \
         patch("sync.invoice_runner._fetch_transitions", return_value=[transition]), \
         patch("sync.invoice_runner._resolve_deal_ids", return_value=[transition]), \
         patch("sync.invoice_runner._update_state"):

        stats = run_invoice_sync(BASE_CFG)

    assert stats["wo_to_in"] == 1
    props_sent = mock_update_fn.call_args[0][1]
    assert "status_code__c" in props_sent
    assert props_sent["status_code__c"].endswith("Z")


def test_run_invoice_sync_no_deal_id_counts_as_failure():
    transition = _make_transition("INV-3", TRANSITION_VO_WO, deal_id=None)

    with patch("sync.invoice_runner.bigquery.Client"), \
         patch("sync.invoice_runner.HubSpotClient"), \
         patch("sync.invoice_runner.make_retrying_update_deal"), \
         patch("sync.invoice_runner._fetch_transitions", return_value=[transition]), \
         patch("sync.invoice_runner._resolve_deal_ids", return_value=[transition]), \
         patch("sync.invoice_runner._update_state"):

        stats = run_invoice_sync(BASE_CFG)

    assert stats["failures"] == 1
    assert stats["successes"] == 0


def test_run_invoice_sync_hubspot_error_counts_as_failure():
    transition = _make_transition("INV-4", TRANSITION_VO_WO)
    mock_update_fn = MagicMock(side_effect=Exception("HubSpot 500"))

    with patch("sync.invoice_runner.bigquery.Client"), \
         patch("sync.invoice_runner.HubSpotClient"), \
         patch("sync.invoice_runner.make_retrying_update_deal", return_value=mock_update_fn), \
         patch("sync.invoice_runner._fetch_transitions", return_value=[transition]), \
         patch("sync.invoice_runner._resolve_deal_ids", return_value=[transition]), \
         patch("sync.invoice_runner._update_state"):

        stats = run_invoice_sync(BASE_CFG)

    assert stats["failures"] == 1
    assert stats["successes"] == 0
