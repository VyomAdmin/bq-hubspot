-- One-time bootstrap: seeds invoice_sync_state from the current snapshot
-- of invoices_unique_view. Run this once before the first scheduled sync.
--
-- After this runs, the next 5-minute poll will detect any VO→WO or WO→IN
-- transitions that occur from this point forward.

INSERT INTO `psychic-lens-456414-e4.omega_stg.invoice_sync_state`
  (id, last_known_status, first_seen_at, last_updated_at)
SELECT
  v.id,
  v.status,
  CURRENT_TIMESTAMP(),
  CURRENT_TIMESTAMP()
FROM `psychic-lens-456414-e4.omega_stg.invoices_unique_view` v
WHERE NOT EXISTS (
  SELECT 1
  FROM `psychic-lens-456414-e4.omega_stg.invoice_sync_state` s
  WHERE s.id = v.id
);

-- Verify row count after seeding:
-- SELECT COUNT(*) FROM `psychic-lens-456414-e4.omega_stg.invoice_sync_state`;
