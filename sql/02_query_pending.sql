-- Parameterised pending-changes query used by the integration service.
-- @window_minutes and @max_rows are injected by the Python client as literals.

DECLARE window_minutes INT64 DEFAULT 10;   -- overlap window (5-min cadence + 5-min buffer)
DECLARE max_rows       INT64 DEFAULT 5000;

SELECT
  row_id,
  object_type,
  hubspot_id,
  business_key,
  business_key_property,
  properties,
  retry_count
FROM `your_project.crm_integration.hubspot_updates`
WHERE
  -- Slight over-fetch vs the 5-min schedule to guard against clock drift
  last_changed_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL window_minutes MINUTE)

  -- Exclude rows currently being processed by a concurrent invocation
  AND sync_status IN ('PENDING', 'FAILED')

  -- Hard cap on retries — permanently failed rows go to DLQ manually
  AND retry_count < 5
ORDER BY
  last_changed_at ASC   -- oldest-first: fairness, avoids starvation
LIMIT max_rows;
