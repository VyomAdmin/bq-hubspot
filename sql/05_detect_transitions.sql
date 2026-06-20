-- Detects VO→WO and WO→IN status transitions by comparing the live view
-- against the last known state we recorded.
--
-- Returns one row per invoice that has a qualifying transition.
-- Run every 5 minutes; safe to re-run (idempotent read).

SELECT
  v.id                          AS invoice_id,
  s.last_known_status           AS previous_status,
  v.status                      AS current_status,

  -- Transition label used by the runner to choose the HubSpot action
  CASE
    WHEN s.last_known_status = 'QO' AND v.status = 'WO' THEN 'QO_TO_WO'
    WHEN s.last_known_status = 'WO' AND v.status = 'IN' THEN 'WO_TO_IN'
  END                           AS transition,

  -- Cached deal ID (may be NULL on first encounter — runner will search)
  s.hubspot_deal_id

FROM `psychic-lens-456414-e4.omega_stg.invoices_unique_view` v
INNER JOIN `psychic-lens-456414-e4.omega_stg.invoice_sync_state` s
  ON v.id = s.id
WHERE
  (s.last_known_status = 'QO' AND v.status = 'WO')
  OR
  (s.last_known_status = 'WO' AND v.status = 'IN')

-- Exclude rows that failed last time and haven't changed again
-- (the runner retries these automatically via last_sync_status)
ORDER BY v.id;


-- ── Companion: seed new invoices into the state table ────────────────────────
-- Run once on bootstrap (or as part of the sync run) to add invoices
-- that aren't tracked yet, starting from their current status.
--
-- INSERT INTO `psychic-lens-456414-e4.omega_stg.invoice_sync_state`
--   (id, last_known_status, first_seen_at, last_updated_at)
-- SELECT
--   v.id,
--   v.status,
--   CURRENT_TIMESTAMP(),
--   CURRENT_TIMESTAMP()
-- FROM `psychic-lens-456414-e4.omega_stg.invoices_unique_view` v
-- WHERE NOT EXISTS (
--   SELECT 1
--   FROM `psychic-lens-456414-e4.omega_stg.invoice_sync_state` s
--   WHERE s.id = v.id
-- );
