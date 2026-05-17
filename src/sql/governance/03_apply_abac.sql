-- Apply ABAC functions to Silver tables.
-- Materialized views (Gold) don't support direct ROW FILTER / SET MASK;
-- governance is applied at Silver and inherited through the pipeline.

-- Row filter on demand_hourly
ALTER TABLE ogp_dev.silver.demand_hourly
SET ROW FILTER ogp_dev.ops.filter_recent_only ON (event_ts_utc);

-- Column mask on weather_hourly.station_name
ALTER TABLE ogp_dev.silver.weather_hourly
ALTER COLUMN station_name
SET MASK ogp_dev.ops.mask_station_name;

-- Verify
DESCRIBE EXTENDED ogp_dev.silver.demand_hourly;
DESCRIBE EXTENDED ogp_dev.silver.weather_hourly;
