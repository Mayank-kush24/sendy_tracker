-- Composite indexes for common filter combinations on ``subscribers``.
-- Optimizer can use these for ``get_users``, ``get_subscribers_by_list``,
-- ``_fetch_subscriber_stats_once``, and ``get_active_subscriber_counts``.
-- Apply once per database.
--
-- Verify e.g.:
--   EXPLAIN SELECT * FROM subscribers
--   WHERE `list` = 1 AND timestamp >= 1700000000 AND confirmed = 1;

ALTER TABLE subscribers
  ADD INDEX idx_sub_list_ts (`list`, timestamp),
  ADD INDEX idx_sub_list_conf_ts (`list`, confirmed, timestamp),
  ADD INDEX idx_sub_active (unsubscribed, bounced, complaint, `list`);
