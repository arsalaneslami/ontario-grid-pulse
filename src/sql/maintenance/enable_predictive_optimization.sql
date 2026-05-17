-- Enable Predictive Optimization at the catalog level.
-- Auto-runs OPTIMIZE, VACUUM, and statistics updates on all tables.
ALTER CATALOG ogp_dev ENABLE PREDICTIVE OPTIMIZATION;

-- Verify
DESCRIBE CATALOG EXTENDED ogp_dev;
