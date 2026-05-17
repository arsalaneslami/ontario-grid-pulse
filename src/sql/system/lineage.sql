-- Table-to-table lineage for this project
SELECT 
  source_table_full_name,
  target_table_full_name,
  event_time
FROM system.access.table_lineage
WHERE source_table_catalog = 'ogp_dev'
   OR target_table_catalog = 'ogp_dev'
ORDER BY event_time DESC
LIMIT 20;

-- Column-level lineage into Gold
SELECT 
  source_table_full_name,
  source_column_name,
  target_table_full_name,
  target_column_name
FROM system.access.column_lineage
WHERE target_table_catalog = 'ogp_dev'
  AND target_table_schema = 'gold'
LIMIT 30;
