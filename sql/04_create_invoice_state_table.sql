-- State-tracking table for invoice status transitions.
-- Because invoices_unique_view is a view (no audit log), we track the last
-- status we observed per invoice so we can detect transitions on each poll.

CREATE TABLE IF NOT EXISTS `psychic-lens-456414-e4.omega_stg.invoice_sync_state`
(
  id STRING NOT NULL,
  last_known_status STRING NOT NULL,
  first_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  last_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  hubspot_deal_id STRING,
  last_sync_status STRING,
  last_sync_error STRING
)
CLUSTER BY id
OPTIONS (
  description = 'Tracks last observed status per invoice for transition detection (VO→WO, WO→IN).'
);