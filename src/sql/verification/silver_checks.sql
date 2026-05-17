-- Silver layer health checks.

-- Demand: row count and UTC time range
SELECT 
  COUNT(*)              AS rows,
  MIN(event_ts_utc)     AS min_ts,
  MAX(event_ts_utc)     AS max_ts
FROM ogp_dev.silver.demand_hourly;

-- Weather: row count, time range, station count
SELECT 
  COUNT(*)              AS rows,
  MIN(event_ts_utc)     AS min_ts,
  MAX(event_ts_utc)     AS max_ts,
  COUNT(DISTINCT station_id) AS stations
FROM ogp_dev.silver.weather_hourly;

-- Documented anomaly: market_demand_mw < ontario_demand_mw 
-- (net-import hours in shoulder seasons)
SELECT 
  event_date_est,
  hour_ending,
  market_demand_mw,
  ontario_demand_mw,
  ontario_demand_mw - market_demand_mw AS gap_mw
FROM ogp_dev.silver.demand_hourly
WHERE market_demand_mw < ontario_demand_mw
ORDER BY event_date_est, hour_ending;
