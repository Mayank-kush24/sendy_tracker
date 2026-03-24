-- ==========================================================================
-- Recommended indexes for sendy_tracker
-- Based on the ACTUAL Sendy schema (verified via show_schema.py)
--
-- Most indexes already exist in Sendy's schema — only the ones marked
-- "MISSING" need to be created.  Run EXPLAIN after creating them.
--
-- InnoDB online DDL: CREATE INDEX does NOT lock the table for reads/writes
-- on MariaDB 10.x.  Safe to run on a live installation.
-- ==========================================================================


-- --------------------------------------------------------------------------
-- Table: subscribers  (11.9 M rows)
--
-- EXISTING indexes (created by Sendy):
--   s_list, s_unsubscribed, s_bounced, s_bounce_soft, s_complaint,
--   s_confirmed, s_timestamp, s_email, s_last_campaign, s_messageid,
--   s_country, s_referrer, s_method, s_added_via, s_gdpr
--
-- Key query patterns in sendy_queries.py
--   get_subscriber_totals():
--       SELECT COUNT(*), SUM(confirmed), SUM(unsubscribed), SUM(bounced)…
--       → full-table aggregate; already fast with InnoDB COUNT(*) stats.
--
--   get_subscriber_growth():
--       WHERE timestamp >= X AND timestamp <= Y
--       GROUP BY DATE_FORMAT(FROM_UNIXTIME(timestamp), …)
--       → s_timestamp index covers the range scan ✓
--
--   get_lists_summary():
--       LEFT JOIN subscribers s ON s.list = li.id
--       → s_list index covers this JOIN ✓
--
--   get_overview_stats() / get_campaigns_stats() — subscriber subqueries:
--       WHERE last_campaign IN (…) — SUM(bounced), SUM(unsubscribed)
--       WHERE last_campaign = c.id — correlated subquery
--       → s_last_campaign index covers both ✓
--
-- MISSING: composite index for GROUP BY list + active-subscriber count
-- --------------------------------------------------------------------------

-- 1. Composite for "active subscribers per list" aggregate
--    Covers: SUM(CASE WHEN confirmed=1 AND unsubscribed=0 AND bounced=0 …)
--            GROUP BY list — used in get_lists_summary()
--    Skips rows where the subscriber is clearly inactive via partial index trick.
CREATE INDEX IF NOT EXISTS idx_sub_list_active
    ON subscribers (list, confirmed, unsubscribed, bounced, complaint);


-- --------------------------------------------------------------------------
-- Table: campaigns  (3 630 rows — small; indexes mainly for future growth)
--
-- EXISTING indexes: PRIMARY (id) only
--
-- Key query patterns:
--   WHERE sent NOT IN ('', '0') AND sent IS NOT NULL
--   AND (CASE WHEN send_date REGEXP … → range on a varchar column)
--   ORDER BY (same CASE expression) DESC
--   LIMIT ? OFFSET ?
--
-- Because send_date is varchar (not DATETIME), the optimizer CANNOT use a
-- B-tree index for the CASE/FROM_UNIXTIME expression.  With only 3 630 rows
-- a full scan is fine; but add the index below so ORDER BY benefits when the
-- table grows.
-- --------------------------------------------------------------------------

-- 2. sent column — filter sent campaigns fast
CREATE INDEX IF NOT EXISTS idx_camp_sent
    ON campaigns (sent);

-- 3. send_date — ORDER BY / GROUP BY (varchar, prefix for partial ordering)
CREATE INDEX IF NOT EXISTS idx_camp_send_date
    ON campaigns (send_date);

-- 4. app — for multi-brand / multi-user filtering
CREATE INDEX IF NOT EXISTS idx_camp_app
    ON campaigns (app);


-- --------------------------------------------------------------------------
-- Table: links  (10 418 rows)
--
-- EXISTING indexes: s_campaign_id (campaign_id), s_ares_emails_id
--
-- Key query pattern:
--   WHERE campaign_id IN (…)  — covered by s_campaign_id ✓
--   SUM(PHP-serialized count from clicks) — no index can help this
-- --------------------------------------------------------------------------
-- No new indexes needed for links.


-- --------------------------------------------------------------------------
-- Table: queue  (761 510 rows)
--
-- EXISTING indexes: s_id (subscriber_id), st_id (sent), s_campaign_id
--
-- Not queried directly by the dashboard — no new indexes needed.
-- --------------------------------------------------------------------------


-- --------------------------------------------------------------------------
-- Table: skipped_emails  (2.4 M rows)
--
-- EXISTING indexes: s_list (list)
-- Not queried by the dashboard — no new indexes needed.
-- --------------------------------------------------------------------------


-- ==========================================================================
-- Verification — run after creating indexes
-- ==========================================================================

-- Subscriber growth range scan (should use s_timestamp)
EXPLAIN
SELECT DATE_FORMAT(FROM_UNIXTIME(timestamp), '%Y-%m') AS period,
       COUNT(*) AS new_subscribers
FROM   subscribers
WHERE  timestamp >= UNIX_TIMESTAMP('2025-01-01')
  AND  timestamp <= UNIX_TIMESTAMP('2025-12-31')
GROUP  BY period;
-- Expected: type=range, key=s_timestamp

-- Lists summary JOIN (should use s_list)
EXPLAIN
SELECT li.id, COUNT(s.id) AS total
FROM   lists li
LEFT   JOIN subscribers s ON s.list = li.id
GROUP  BY li.id;
-- Expected: subscribers key=s_list

-- Correlated subquery for bounces per campaign (should use s_last_campaign)
EXPLAIN
SELECT SUM(bounced) AS bounces
FROM   subscribers
WHERE  last_campaign = 42;
-- Expected: type=ref, key=s_last_campaign

-- Sent campaigns filter (should use idx_camp_sent)
EXPLAIN
SELECT id, recipients
FROM   campaigns
WHERE  sent IS NOT NULL AND sent NOT IN ('', '0')
ORDER  BY send_date DESC;
-- Expected: key=idx_camp_sent or idx_camp_send_date


-- ==========================================================================
-- Maintenance — after large bulk imports
-- ==========================================================================
-- ANALYZE TABLE subscribers, campaigns, links, queue;
-- ==========================================================================
