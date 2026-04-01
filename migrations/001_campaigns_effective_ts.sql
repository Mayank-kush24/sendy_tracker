-- Stored generated column + index for Sendy ``campaigns`` send-time filtering
-- and ORDER BY. Apply once per database (MySQL 5.7.6+ / 8.x).
--
-- Matches logic previously inlined via ``campaign_effective_unix_ts_sql`` in
-- ``queries.py`` (sent epoch, send_date epoch string, or YYYY-MM-DD prefix).

ALTER TABLE campaigns
  ADD COLUMN effective_ts INT UNSIGNED GENERATED ALWAYS AS (
    IF(
      CAST(sent AS UNSIGNED) BETWEEN 1000000000 AND 2100000000,
      CAST(sent AS UNSIGNED),
      IF(
        send_date REGEXP '^[0-9]{9,11}$',
        CAST(send_date AS UNSIGNED),
        IF(
          CHAR_LENGTH(TRIM(COALESCE(send_date,''))) >= 10
            AND SUBSTRING(TRIM(send_date), 5, 1) = '-',
          UNIX_TIMESTAMP(STR_TO_DATE(SUBSTRING(TRIM(send_date),1,10),'%Y-%m-%d')),
          NULL
        )
      )
    )
  ) STORED,
  ADD INDEX idx_camp_effective_ts (effective_ts);
