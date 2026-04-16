-- =============================================================================
-- ZINIER: Rollback Script - Remove Imported Exchange Rates
-- =============================================================================
-- Run this ONLY if you need to undo the exchange rate import.
-- This removes all rates created by the import script.
--
-- Usage: psql <database_name> -f rollback.sql
-- =============================================================================

-- Step 1: Count records to be deleted (preview)
SELECT 'Records to be deleted:' as action, COUNT(*) as count
FROM res_currency_rate
WHERE name < '2025-08-31'
  AND create_date >= '2026-04-16';

-- Step 2: Delete imported rates
-- These are rates with dates before Aug 2025 that were created after Apr 16, 2026
DELETE FROM res_currency_rate
WHERE name < '2025-08-31'
  AND create_date >= '2026-04-16';

-- Step 3: Verify
SELECT 'Remaining rates:' as status, COUNT(*) as count FROM res_currency_rate;
SELECT 'Earliest rate:' as status, MIN(name)::text as value FROM res_currency_rate;
SELECT 'Latest rate:' as status, MAX(name)::text as value FROM res_currency_rate;

-- =============================================================================
-- NOTE: This rollback is safe because:
-- - Only res_currency_rate records are affected
-- - No journal entries were modified by the import
-- - The original rates (Nov 2025 onwards) remain untouched
-- =============================================================================
