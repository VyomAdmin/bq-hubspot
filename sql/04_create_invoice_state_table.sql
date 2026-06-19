-- State-tracking table for invoice status transitions.
-- Because invoices_unique_view is a view (no audit log), we track the last
-- status we observed per invoice so we can detect transitions on each poll.

CREATE TABLE IF NOT EXISTS `psychic-lens-456414-e4.omega_stg.invoice_sync_state`
(
  -- Invoice identifier — matches invoices_unique_view.id
  id                STRING    NOT NULL,

  -- Last status we successfully observed and synced
  last_known_status STRING    NOT NULL,

  -- Timestamps
  first_seen_at     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
  last_updated_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),

  -- Last HubSpot deal ID resolved for this invoice (cached to avoid repeat lookups)
  hubspot_deal_id   STRING,

  -- Track the last sync outcome per record
  last_sync_status  STRING,   -- 'SUCCESS' | 'FAILED' | NULL
  last_sync_error   STRING
)
-- Cluster on id for fast point-lookups during the JOIN
CLUSTER BY id
OPTIONS (
  description = 'Tracks last observed status per invoice for transition detection (VO→WO, WO→IN).'
);
