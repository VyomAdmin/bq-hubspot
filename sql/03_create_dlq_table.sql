-- Dead-letter table for rows that have exhausted all retries (retry_count >= 5).
-- A scheduled query or Dataflow job should move permanently-failed rows here
-- for manual triage without polluting the main delta table's query performance.

CREATE TABLE IF NOT EXISTS `your_project.crm_integration.hubspot_updates_dlq`
(
  row_id                  STRING    NOT NULL,
  object_type             STRING    NOT NULL,
  hubspot_id              STRING,
  business_key            STRING,
  business_key_property   STRING,
  properties              JSON      NOT NULL,
  last_changed_at         TIMESTAMP NOT NULL,
  sync_attempted_at       TIMESTAMP,
  sync_error              STRING,
  retry_count             INT64     NOT NULL,
  dlq_inserted_at         TIMESTAMP NOT NULL    DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(dlq_inserted_at)
OPTIONS (
  description = 'Dead-letter queue for BigQuery→HubSpot rows that exceeded max retries.'
);

-- Companion query: move exhausted rows from the main table to the DLQ
-- Run this as a scheduled query (e.g. daily) or after each sync run.
/*
INSERT INTO `your_project.crm_integration.hubspot_updates_dlq`
SELECT
  row_id, object_type, hubspot_id, business_key, business_key_property,
  properties, last_changed_at, sync_attempted_at, sync_error, retry_count
FROM `your_project.crm_integration.hubspot_updates`
WHERE sync_status = 'FAILED' AND retry_count >= 5;

DELETE FROM `your_project.crm_integration.hubspot_updates`
WHERE sync_status = 'FAILED' AND retry_count >= 5;
*/
