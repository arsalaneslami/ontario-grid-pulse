# Databricks notebook source
# This line was added locally and deployed via Databricks Asset Bundle
# MAGIC %md
# MAGIC ### 1 - Header / module imports

# COMMAND ----------

"""
silver_pipeline.py
==================
Lakeflow Spark Declarative Pipeline — Silver layer for Ontario Grid Pulse.

Sources:
  ogp_dev.bronze.ieso_demand_raw
  ogp_dev.bronze.eccc_weather_raw

Targets (set by pipeline configuration, written to target schema):
  silver.demand_hourly
  silver.weather_hourly

Transformations:
  - Reconcile timestamps to UTC
    (IESO: EST + 5 hours; ECCC: LST + 5 hours; both UTC−5 with no DST)
  - Rename/cast for analytics-friendly schemas
  - Add derived metrics (heating/cooling degree hours)
  - Apply data quality expectations
  - Stamp audit columns
"""

import dlt
from pyspark.sql.functions import current_timestamp, lit, expr

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2 - Silver table 1 - demand_hourly

# COMMAND ----------

@dlt.table(
    name="demand_hourly",
    comment="Hourly Ontario electricity demand, normalized to UTC. Source: IESO PUB_Demand_YYYY.csv files.",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true",
        "delta.enableChangeDataFeed": "true"
    }
)
@dlt.expect_or_drop("valid_timestamp",  "event_ts_utc IS NOT NULL")
@dlt.expect("valid_demand_range",       "ontario_demand_mw BETWEEN 8000 AND 30000")
@dlt.expect("market_geq_ontario",       "market_demand_mw >= ontario_demand_mw")
@dlt.expect("hour_within_range",        "hour_ending BETWEEN 1 AND 24")
def demand_hourly():
    """
    IESO publishes hour-ending values 1–24 in EST (UTC−5, no DST).
    Period START in UTC = (event_date midnight EST) + (hour_ending − 1) hours + 5 hours
    Example: 2024-01-15, hour 1 → period 00:00–01:00 EST → period start 05:00 UTC
    """
    return (
        spark.readStream.table("ogp_dev.bronze.ieso_demand_raw")
            .selectExpr(
                """
                CAST(event_date AS TIMESTAMP)
                  + make_interval(0, 0, 0, 0, hour_ending - 1, 0, 0)
                  + INTERVAL '5 HOURS'             AS event_ts_utc
                """,
                "event_date              AS event_date_est",
                "hour_ending",
                "market_demand_mw",
                "ontario_demand_mw",
                "_source_file"
            )
            .withColumn("_silver_ts",    current_timestamp())
            .withColumn("_source_table", lit("ogp_dev.bronze.ieso_demand_raw"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3 - Silver table 2 - weather_hourly

# COMMAND ----------

@dlt.table(
    name="weather_hourly",
    comment="Hourly weather observations normalized to UTC with derived degree-hour metrics. Source: ECCC bulk weather data.",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true",
        "delta.enableChangeDataFeed": "true"
    }
)
@dlt.expect_or_drop("valid_timestamp",  "event_ts_utc IS NOT NULL")
@dlt.expect_or_drop("valid_station",    "station_id IS NOT NULL")
@dlt.expect("temp_within_range",        "temp_c IS NULL OR temp_c BETWEEN -50 AND 50")
@dlt.expect("humidity_valid",           "rel_humidity_pct IS NULL OR rel_humidity_pct BETWEEN 0 AND 100")
@dlt.expect("pressure_within_range",    "stn_pressure_kpa IS NULL OR stn_pressure_kpa BETWEEN 90 AND 110")
def weather_hourly():
    """
    ECCC publishes timestamps in LST (Local Standard Time, UTC−5 year-round, no DST).
    Bronze stored the timestamp as-is, Spark tagged it as UTC.
    Real UTC = stored timestamp + 5 hours.
    
    Derived columns:
      hdh — heating degree hours (max(0, 18 − temp_c))
      cdh — cooling degree hours (max(0, temp_c − 18))
    Base 18°C is the Canadian convention for degree-day calculations.
    """
    return (
        spark.readStream.table("ogp_dev.bronze.eccc_weather_raw")
            .selectExpr(
                "event_ts_lst + INTERVAL '5 HOURS'  AS event_ts_utc",
                "climate_id                          AS station_id",
                "station_name",
                "latitude",
                "longitude",
                "temp_c",
                "dew_point_c",
                "rel_humidity_pct",
                "wind_speed_kmh",
                "wind_dir_10s_deg",
                "precip_mm",
                "stn_pressure_kpa",
                "humidex",
                "wind_chill",
                "weather_desc",
                "_source_file"
            )
            .withColumn("hdh", expr("greatest(CAST(0 AS DOUBLE), 18 - temp_c)"))
            .withColumn("cdh", expr("greatest(CAST(0 AS DOUBLE), temp_c - 18)"))
            .withColumn("_silver_ts",    current_timestamp())
            .withColumn("_source_table", lit("ogp_dev.bronze.eccc_weather_raw"))
    )