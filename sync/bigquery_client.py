import logging
from datetime import datetime, timezone

from google.cloud import bigquery

logger = logging.getLogger(__name__)


class BigQueryClient:
    def __init__(self, project_id: str, dataset: str, table: str):
        self.client = bigquery.Client(project=project_id)
        self.project_id = project_id
        self.dataset = dataset
        self.table = table
        self._fqt = f"`{project_id}.{dataset}.{table}`"

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def fetch_pending(self, window_minutes: int, max_rows: int) -> list[dict]:
        """
        Returns rows ready to sync: changed within the last window_minutes,
        not yet successfully synced, and under the retry cap.
        Rows marked IN_PROGRESS by a concurrent run are excluded.
        """
        query = f"""
            SELECT
              row_id,
              object_type,
              hubspot_id,
              business_key,
              business_key_property,
              properties,
              retry_count
            FROM {self._fqt}
            WHERE
              last_changed_at >= TIMESTAMP_SUB(
                CURRENT_TIMESTAMP(), INTERVAL {window_minutes} MINUTE
              )
              AND sync_status IN ('PENDING', 'FAILED')
              AND retry_count < 5
            ORDER BY last_changed_at ASC
            LIMIT {max_rows}
        """
        logger.info("Fetching pending rows (window=%dm, limit=%d)", window_minutes, max_rows)
        results = self.client.query(query).result()
        rows = []
        for row in results:
            r = dict(row)
            # BigQuery JSON columns come back as strings — parse if needed
            if isinstance(r.get("properties"), str):
                import json
                r["properties"] = json.loads(r["properties"])
            rows.append(r)
        logger.info("Fetched %d pending rows", len(rows))
        return rows

    # ------------------------------------------------------------------
    # Write — in-progress lock
    # ------------------------------------------------------------------

    def mark_in_progress(self, row_ids: list[str]) -> None:
        if not row_ids:
            return
        ids_literal = ", ".join(f"'{_esc(r)}'" for r in row_ids)
        self.client.query(f"""
            UPDATE {self._fqt}
            SET
              sync_status       = 'IN_PROGRESS',
              sync_attempted_at = CURRENT_TIMESTAMP()
            WHERE row_id IN ({ids_literal})
        """).result()
        logger.debug("Marked %d rows IN_PROGRESS", len(row_ids))

    # ------------------------------------------------------------------
    # Write — results
    # ------------------------------------------------------------------

    def apply_results(
        self,
        successes: list[str],
        failures: list[tuple[str, str]],  # (row_id, error_message)
    ) -> None:
        """
        Writes sync outcomes back using a MERGE so retry_count increments
        atomically and there are no partial-update races.
        Uses a short-lived temp table for the staging data.
        """
        rows_to_update: list[dict] = []
        for row_id in successes:
            rows_to_update.append(
                {"row_id": row_id, "sync_status": "SUCCESS", "sync_error": None}
            )
        for row_id, error in failures:
            rows_to_update.append(
                {
                    "row_id": row_id,
                    "sync_status": "FAILED",
                    "sync_error": error[:1024],
                }
            )

        if not rows_to_update:
            return

        tmp = f"{self.project_id}.{self.dataset}._sync_results_{_ts()}"
        schema = [
            bigquery.SchemaField("row_id", "STRING"),
            bigquery.SchemaField("sync_status", "STRING"),
            bigquery.SchemaField("sync_error", "STRING"),
        ]
        job_cfg = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition="WRITE_TRUNCATE",
        )
        self.client.load_table_from_json(rows_to_update, tmp, job_config=job_cfg).result()
        logger.debug("Loaded %d result rows into temp table %s", len(rows_to_update), tmp)

        self.client.query(f"""
            MERGE {self._fqt} T
            USING `{tmp}` S ON T.row_id = S.row_id
            WHEN MATCHED AND S.sync_status = 'SUCCESS' THEN
              UPDATE SET
                T.sync_status       = 'SUCCESS',
                T.sync_error        = NULL,
                T.sync_succeeded_at = CURRENT_TIMESTAMP()
            WHEN MATCHED AND S.sync_status = 'FAILED' THEN
              UPDATE SET
                T.sync_status = 'FAILED',
                T.sync_error  = S.sync_error,
                T.retry_count = T.retry_count + 1
        """).result()

        try:
            self.client.delete_table(tmp)
        except Exception:
            logger.warning("Could not delete temp table %s; clean up manually", tmp)

        logger.info(
            "Applied results: %d successes, %d failures",
            len(successes),
            len(failures),
        )


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f")


def _esc(s: str) -> str:
    """Minimal SQL string escape — replaces single quotes."""
    return s.replace("'", "\\'")
