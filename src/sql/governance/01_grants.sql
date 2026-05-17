-- Grants for the Ontario Grid Pulse catalog.
-- Run after the catalog and schemas exist.
-- Read-only analyst pattern on Gold layer.

GRANT USE CATALOG ON CATALOG ogp_dev TO `account users`;
GRANT USE SCHEMA  ON SCHEMA  ogp_dev.gold TO `account users`;
GRANT SELECT      ON SCHEMA  ogp_dev.gold TO `account users`;

-- Verify
SHOW GRANTS ON CATALOG ogp_dev;
SHOW GRANTS ON SCHEMA ogp_dev.gold;
