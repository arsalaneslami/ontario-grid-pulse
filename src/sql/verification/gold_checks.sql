-- Gold layer health checks and analytical highlights.

-- Row count and time range
SELECT 
  COUNT(*)            AS rows,
  MIN(event_ts_utc)   AS min_ts,
  MAX(event_ts_utc)   AS max_ts
FROM ogp_dev.gold.demand_weather_hourly;

-- 24-hour demand pattern for a sample day
SELECT 
  event_ts_utc,
  hour_of_day_local,
  is_weekend,
  temp_c,
  temp_category,
  ontario_demand_mw,
  net_export_mw
FROM ogp_dev.gold.demand_weather_hourly
WHERE event_date_local = '2024-07-02'
ORDER BY hour_of_day_local;

-- The analytical payoff: demand vs temperature category
SELECT 
  temp_category,
  COUNT(*) AS hours,
  ROUND(AVG(ontario_demand_mw)) AS avg_demand_mw,
  MIN(temp_c) AS min_temp_c,
  MAX(temp_c) AS max_temp_c
FROM ogp_dev.gold.demand_weather_hourly
GROUP BY temp_category
ORDER BY MIN(temp_c);
