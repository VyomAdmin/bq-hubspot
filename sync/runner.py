import logging
import time
from itertools import islice
from typing import Any

from sync.bigquery_client import BigQueryClient
from sync.config import Config
from sync.hubspot_client import HubSpotClient, make_retrying_batch_update
from sync.mapper import build_batch_payload

logger = logging.getLogger(__name__)


def run_sync(cfg: Config) -> dict[str, Any]:
    """
    Main orchestration loop for a single 5-minute sync run.

    Returns a stats dict suitable for structured logging and the HTTP response.
    """
    run_start = time.monotonic()
    bq = BigQueryClient(cfg.project_id, cfg.bq_dataset, cfg.bq_table)
    hs = HubSpotClient(cfg.hubspot_token, cfg.max_requests_per_minute)
    retrying_update = make_retrying_batch_update(hs)

    stats: dict[str, Any] = {
        "fetched": 0,
        "skipped": 0,
        "batches": 0,
        "successes": 0,
        "failures": 0,
        "duration_ms": 0,
    }

    # 1. Fetch rows pending sync
    rows = bq.fetch_pending(cfg.window_minutes, cfg.max_rows_per_run)
    stats["fetched"] = len(rows)

    if not rows:
        logger.info("sync_complete — nothing to sync", extra=stats)
        return stats

    # 2. Claim rows to prevent double-processing by concurrent invocations
    bq.mark_in_progress([r["row_id"] for r in rows])

    # 3. Group by object_type and process each type independently
    by_type: dict[str, list[dict]] = {}
    for row in rows:
        by_type.setdefault(row["object_type"], []).append(row)

    all_successes: list[str] = []
    all_failures: list[tuple[str, str]] = []

    for object_type, type_rows in by_type.items():
        logger.info(
            "Processing object_type=%s rows=%d", object_type, len(type_rows)
        )
        mapped = build_batch_payload(type_rows, object_type, cfg.mappings)
        stats["skipped"] += len(type_rows) - len(mapped)

        for batch in _chunk(mapped, cfg.batch_size):
            stats["batches"] += 1
            hs_inputs = [item["payload"] for item in batch]
            # Map HubSpot-facing id → our row_id for result writeback
            id_to_row: dict[str, str] = {item["hs_id"]: item["row_id"] for item in batch}

            try:
                response = retrying_update(object_type, hs_inputs)
                _parse_response(response, id_to_row, all_successes, all_failures)
            except Exception as exc:
                # Batch exhausted retries — mark every row in it as failed
                logger.error(
                    "batch_failed object_type=%s error=%s", object_type, exc
                )
                for item in batch:
                    all_failures.append((item["row_id"], str(exc)))

    # 4. Persist outcomes to BigQuery
    bq.apply_results(all_successes, all_failures)

    stats["successes"] = len(all_successes)
    stats["failures"] = len(all_failures)
    stats["duration_ms"] = int((time.monotonic() - run_start) * 1000)

    logger.info("sync_complete", extra=stats)
    return stats


def _parse_response(
    response: dict[str, Any],
    id_to_row: dict[str, str],
    successes: list[str],
    failures: list[tuple[str, str]],
) -> None:
    """
    Splits a HubSpot batch/update response (200 or 207) into per-row outcomes.

    HubSpot 207 shape:
      { "results": [{"id": "...", ...}], "errors": [{"id": "...", "message": "..."}] }
    """
    for result in response.get("results", []):
        row_id = id_to_row.get(str(result.get("id", "")))
        if row_id:
            successes.append(row_id)

    for error in response.get("errors", []):
        # HubSpot surfaces the failing id in different places depending on error type
        hs_id = (
            str(error.get("id", ""))
            or str((error.get("context") or {}).get("id", [""])[0])
        )
        row_id = id_to_row.get(hs_id)
        msg = error.get("message", "unknown HubSpot error")
        if row_id:
            failures.append((row_id, msg))
        else:
            logger.warning(
                "Could not map HubSpot error back to a row_id: %s", error
            )


def _chunk(lst: list, size: int):
    it = iter(lst)
    while batch := list(islice(it, size)):
        yield batch
