-- Proactive indexes on ``skipped_emails`` for email / (app, email) lookups.
-- Table can hold millions of rows; ``s_list`` alone is not enough for suppression
-- checks by address. No application SQL changes required yet.

ALTER TABLE skipped_emails
  ADD INDEX idx_skipped_email (email),
  ADD INDEX idx_skipped_app_email (app, email);
