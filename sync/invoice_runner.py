"""
Invoice status transition sync: BigQuery invoices_unique_view → HubSpot Deals.

Detects two transitions every 5-minute poll:
  VO → WO : set deal stage to WON  (pipeline 691581097, stage 1013210905)
  WO → IN : set status_code__c = current UTC ISO timestamp (Install Completed)

Deals are looked up via omega_job__c = invoices_unique_view.id.
"""

import logging
import time
from datetime import datetime, timezone
from itertools import islice
from typing import Any

from google.cloud import bigquery

from sync.config import Config
from sync.hubspot_client import HubSpotClient, make_retrying_update_deal

logger = logging.getLogger(__name__)

TRANSITION_VO_WO = "VO_TO_WO"
TRANSITION_WO_IN = "WO_TO_IN"


# ---------------------------------------------------------------------------
# Public entrypoint
# ---------------------------------------------------------------------------

def run_invoice_sync(cfg: Config) -> dict[str, Any]:
    """
    Main entrypoint called by the Flask handler.
    Returns a stats dict for structured logging and the HTTP response.
    """
    run_start = time.monotonic()
    bq = bigquery.Client(project=cfg.invoice_project)
    hs = HubSpotClient(cfg.hubspot_token, cfg.max_requests_per_minute)
    retrying_update = make_retrying_update_deal(hs)

    stats: dict[str, Any] = {
        "transitions_detected": 0,
        "vo_to_wo": 0,
        "wo_to_in": 0,
        "successes": 0,
        "failures": 0,
        "duration_ms": 0,
    }

    # 1. Detect qualifying transitions
    transitions = _fetch_transitions(bq, cfg)
    stats["transitions_detected"] = len(transitions)

    if not transitions:
        logger.info("invoice_sync_complete — no transitions", extra=stats)
        return stats

    # 2. Resolve HubSpot deal IDs for invoices without a cached deal ID.
    #    Group invoice IDs that need lookup, call search API in batches of 100.
    transitions = _resolve_deal_ids(transitions, hs, cfg)

    # 3. Process each transition
    successes: list[tuple[str, str]] = []  # (invoice_id, bq_updated_value)
    failures: list[tuple[str, str, str]] = []  # (invoice_id, transition, error)

    for row in transitions:
        invoice_id = row["invoice_id"]
        transition = row["transition"]
        deal_id = row.get("hubspot_deal_id")

        if not deal_id:
            msg = f"No HubSpot deal found for omega_job__c={invoice_id}"
            logger.warning("deal_not_found invoice_id=%s", invoice_id)
            failures.append((invoice_id, transition, msg))
            continue

        try:
            properties = _build_properties(transition, cfg)

            # Skip if HubSpot already has the target value
            if _already_synced(hs, deal_id, transition, properties, cfg):
                logger.info(
                    "deal_skipped invoice_id=%s deal_id=%s transition=%s — already up to date",
                    invoice_id, deal_id, transition,
                )
                successes.append((invoice_id, properties["bq_updated"]))
                if transition == TRANSITION_VO_WO:
                    stats["vo_to_wo"] += 1
                else:
                    stats["wo_to_in"] += 1
                continue

            retrying_update(deal_id, properties)
            successes.append((invoice_id, properties["bq_updated"]))
            if transition == TRANSITION_VO_WO:
                stats["vo_to_wo"] += 1
            else:
                stats["wo_to_in"] += 1
            logger.info(
                "deal_updated invoice_id=%s deal_id=%s transition=%s",
                invoice_id, deal_id, transition,
            )
        except Exception as exc:
            logger.error(
                "deal_update_failed invoice_id=%s deal_id=%s transition=%s error=%s",
                invoice_id, deal_id, transition, exc,
            )
            failures.append((invoice_id, transition, str(exc)))

    # 4. Persist state updates back to BigQuery
    _update_state(bq, cfg, successes, failures, transitions)

    stats["successes"] = len(successes)
    stats["failures"] = len(failures)
    stats["duration_ms"] = int((time.monotonic() - run_start) * 1000)
    logger.info("invoice_sync_complete", extra=stats)
    return stats


# ---------------------------------------------------------------------------
# BigQuery helpers
# ---------------------------------------------------------------------------

def _fqt(project: str, dataset: str, table: str) -> str:
    return f"`{project}.{dataset}.{table}`"


def _fetch_transitions(bq: bigquery.Client, cfg: Config) -> list[dict]:
    view = _fqt(cfg.invoice_project, cfg.invoice_dataset, cfg.invoice_view)
    state = _fqt(cfg.invoice_project, cfg.invoice_dataset, cfg.invoice_state_table)

    query = f"""
        SELECT
          v.id                AS invoice_id,
          s.last_known_status AS previous_status,
          v.status            AS current_status,
          CASE
            WHEN s.last_known_status = 'VO' AND v.status = 'WO' THEN '{TRANSITION_VO_WO}'
            WHEN s.last_known_status = 'WO' AND v.status = 'IN' THEN '{TRANSITION_WO_IN}'
          END                 AS transition,
          s.hubspot_deal_id
        FROM {view} v
        INNER JOIN {state} s ON CAST(v.id AS STRING) = s.id
        WHERE
          (s.last_known_status = 'VO' AND v.status = 'WO')
          OR (s.last_known_status = 'WO' AND v.status = 'IN')
        ORDER BY v.id
    """
    logger.info("Fetching invoice transitions")
    rows = [dict(r) for r in bq.query(query).result()]
    logger.info("Found %d transitions", len(rows))
    return rows


def _update_state(
    bq: bigquery.Client,
    cfg: Config,
    successes: list[tuple[str, str]],
    failures: list[tuple[str, str, str]],
    transitions: list[dict],
) -> None:
    """
    Updates invoice_sync_state:
      - Successes: advance last_known_status to current_status, clear error
      - Failures: record error, leave last_known_status unchanged (will retry)
    Also caches any newly resolved hubspot_deal_ids.
    """
    if not transitions:
        return

    state = _fqt(cfg.invoice_project, cfg.invoice_dataset, cfg.invoice_state_table)
    success_map = {iid: bq_updated for iid, bq_updated in successes}
    failure_map = {f[0]: f[2] for f in failures}

    # Build a temp update dataset
    rows: list[dict] = []
    for t in transitions:
        iid = t["invoice_id"]
        if iid in success_map:
            rows.append({
                "id": str(iid),
                "new_status": t["current_status"],
                "hubspot_deal_id": t.get("hubspot_deal_id"),
                "last_sync_status": "SUCCESS",
                "last_sync_error": None,
                "bq_updated": success_map[iid],
            })
        elif iid in failure_map:
            rows.append({
                "id": str(iid),
                "new_status": t["previous_status"],  # keep old status on failure
                "hubspot_deal_id": t.get("hubspot_deal_id"),
                "last_sync_status": "FAILED",
                "last_sync_error": failure_map[iid][:1024],
                "bq_updated": None,
            })

    if not rows:
        return

    tmp = f"{cfg.invoice_project}.{cfg.invoice_dataset}._inv_sync_results_{_ts()}"
    schema = [
        bigquery.SchemaField("id", "STRING"),
        bigquery.SchemaField("new_status", "STRING"),
        bigquery.SchemaField("hubspot_deal_id", "STRING"),
        bigquery.SchemaField("last_sync_status", "STRING"),
        bigquery.SchemaField("last_sync_error", "STRING"),
        bigquery.SchemaField("bq_updated", "STRING"),
    ]
    bq.load_table_from_json(
        rows, tmp,
        job_config=bigquery.LoadJobConfig(
            schema=schema, write_disposition="WRITE_TRUNCATE"
        ),
    ).result()

    bq.query(f"""
        MERGE {state} T
        USING `{tmp}` S ON T.id = S.id
        WHEN MATCHED THEN UPDATE SET
          T.last_known_status = S.new_status,
          T.hubspot_deal_id   = COALESCE(S.hubspot_deal_id, T.hubspot_deal_id),
          T.last_updated_at   = CURRENT_TIMESTAMP(),
          T.last_sync_status  = S.last_sync_status,
          T.last_sync_error   = S.last_sync_error,
          T.bq_updated        = S.bq_updated
    """).result()

    try:
        bq.delete_table(tmp)
    except Exception:
        logger.warning("Could not delete temp table %s", tmp)

    logger.info("State updated: %d successes, %d failures", len(success_map), len(failures))


def _seed_new_invoices(bq: bigquery.Client, cfg: Config) -> int:
    """
    Inserts invoices not yet tracked into invoice_sync_state from their current status.
    Called automatically at the start of each run so new records are picked up.
    Returns count of newly seeded rows.
    """
    view = _fqt(cfg.invoice_project, cfg.invoice_dataset, cfg.invoice_view)
    state = _fqt(cfg.invoice_project, cfg.invoice_dataset, cfg.invoice_state_table)

    result = bq.query(f"""
        INSERT INTO {state} (id, last_known_status, first_seen_at, last_updated_at)
        SELECT
          CAST(v.id AS STRING),
          v.status,
          CURRENT_TIMESTAMP(),
          CURRENT_TIMESTAMP()
        FROM {view} v
        WHERE NOT EXISTS (
          SELECT 1 FROM {state} s WHERE s.id = CAST(v.id AS STRING)
        )
    """).result()

    count = result.num_dml_affected_rows or 0
    if count:
        logger.info("Seeded %d new invoices into sync state", count)
    return count


# ---------------------------------------------------------------------------
# HubSpot helpers
# ---------------------------------------------------------------------------

def _resolve_deal_ids(
    transitions: list[dict], hs: HubSpotClient, cfg: Config
) -> list[dict]:
    """
    For any transition row missing a hubspot_deal_id, batch-search HubSpot
    deals by omega_job__c and fill in the deal ID.
    """
    need_lookup = [t for t in transitions if not t.get("hubspot_deal_id")]
    if not need_lookup:
        return transitions

    invoice_ids = [str(t["invoice_id"]) for t in need_lookup]
    logger.info("Looking up %d deal IDs via HubSpot search", len(invoice_ids))

    # Search in chunks of 100 (HubSpot IN filter limit)
    resolved: dict[str, str] = {}
    for chunk in _chunk(invoice_ids, 100):
        try:
            resolved.update(
                hs.search_deals_by_property(cfg.hs_omega_job_property, chunk)
            )
        except Exception as exc:
            logger.error("HubSpot deal search failed: %s", exc)

    # Merge resolved IDs back into transition rows
    for t in transitions:
        if not t.get("hubspot_deal_id"):
            t["hubspot_deal_id"] = resolved.get(str(t["invoice_id"]))

    return transitions


def _already_synced(
    hs: HubSpotClient, deal_id: str, transition: str, properties: dict, cfg: Config
) -> bool:
    """Returns True if HubSpot already has the target value for this transition."""
    try:
        if transition == TRANSITION_VO_WO:
            current = hs.get_deal(deal_id, ["dealstage"])
            return current.get("dealstage") == cfg.hs_won_stage
        if transition == TRANSITION_WO_IN:
            current = hs.get_deal(deal_id, [cfg.hs_install_completed_property])
            return current.get(cfg.hs_install_completed_property) == properties[cfg.hs_install_completed_property]
    except Exception as exc:
        logger.warning("Could not fetch deal %s for idempotency check: %s", deal_id, exc)
    return False


def _build_properties(transition: str, cfg: Config) -> dict[str, Any]:
    """Returns the HubSpot property payload for a given transition."""
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    if transition == TRANSITION_VO_WO:
        return {
            "dealstage": cfg.hs_won_stage,
            "pipeline": cfg.hs_deal_pipeline,
            "bq_updated": f"WON - {now_iso}",
        }
    if transition == TRANSITION_WO_IN:
        return {
            cfg.hs_install_completed_property: "Install Completed",
            "bq_updated": f"Install Completed - {now_iso}",
        }
    raise ValueError(f"Unknown transition: {transition}")


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def _chunk(lst: list, size: int):
    it = iter(lst)
    while batch := list(islice(it, size)):
        yield batch


def _ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f")
