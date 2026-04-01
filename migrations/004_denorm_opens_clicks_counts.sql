-- Denormalized comma-entry counts for ``campaigns.opens`` and ``links.clicks``.
-- Application code uses these when ``queries.USE_DENORM_COUNTS`` is True (see
-- ``queries._comma_count`` / ``_comma_count_col``).
--
-- Sendy’s PHP (not this repo) appends to ``opens`` / ``clicks``. Keep
-- ``opens_count`` / ``clicks_count`` in sync on every write (recompute with the
-- same CASE/LENGTH/REPLACE formula, or increment alongside each append), or add
-- MySQL triggers on UPDATE. Otherwise stats will drift after new opens/clicks.

ALTER TABLE campaigns
  ADD COLUMN opens_count INT NOT NULL DEFAULT 0;

ALTER TABLE links
  ADD COLUMN clicks_count INT NOT NULL DEFAULT 0;

UPDATE campaigns SET opens_count =
  CASE WHEN opens IS NULL OR opens = '' THEN 0
  ELSE LENGTH(opens) - LENGTH(REPLACE(opens, ',', '')) + 1 END
WHERE opens_count = 0;

UPDATE links SET clicks_count =
  CASE WHEN clicks IS NULL OR clicks = '' THEN 0
  ELSE LENGTH(clicks) - LENGTH(REPLACE(clicks, ',', '')) + 1 END
WHERE clicks_count = 0;
