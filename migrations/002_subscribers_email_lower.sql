-- Normalized email for index-friendly filters and DISTINCT counts on ``subscribers``.
-- Apply once per database (MySQL 5.7.6+ / 8.x). See ``queries.py``
-- ``_subscriber_where_fragment``, ``count_users_distinct_email``, ``get_users_by_email``.

ALTER TABLE subscribers
  ADD COLUMN email_lower VARCHAR(100)
    GENERATED ALWAYS AS (LOWER(TRIM(email))) STORED,
  ADD INDEX idx_sub_email_lower (email_lower);
