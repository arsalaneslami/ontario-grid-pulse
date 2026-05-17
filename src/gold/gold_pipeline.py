# Databricks notebook source
# MAGIC %md
# MAGIC ### 1 - Imports and header

# COMMAND ----------

"""
gold_pipeline.py
================
Lakeflow Spark Declarative Pipeline — Gold layer for Ontario Grid Pulse.

Sources:
  ogp_dev.silver.demand_hourly
  ogp_dev.silver.weather_hourly

Targets (set by pipeline configuration):
  gold.demand_weather_hourly

Materialization: Materialized view (full refresh).
Cluster: Liquid Clustering on event_ts_utc.
"""

import dlt
from pyspark.sql.functions import (
    current_timestamp, col, expr,
    dayofweek, hour, month, year,
    when
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2 - demand_weather_hourly Gold table

# COMMAND ----------

@dlt.table(
    name="demand_weather_hourly",
    comment=(
        "Hourly demand joined to weather observations, enriched with calendar "
        "features and derived metrics. Intended as the primary feature table for "
        "demand forecasting and weather-correlation analytics."
    ),
    cluster_by=["event_ts_utc"],
    table_properties={
        "quality": "gold",
        "delta.enableChangeDataFeed": "true",
        "pipelines.autoOptimize.managed": "true"
    }
)
@dlt.expect_or_drop("valid_timestamp",  "event_ts_utc IS NOT NULL")
@dlt.expect("temp_in_plausible_range",  "temp_c IS NULL OR temp_c BETWEEN -50 AND 50")
@dlt.expect("demand_in_plausible_range","ontario_demand_mw BETWEEN 5000 AND 35000")
@dlt.expect("station_present",          "station_id IS NOT NULL")
def demand_weather_hourly():
    """
    Inner join of silver.demand_hourly and silver.weather_hourly on event_ts_utc.
    Result is restricted to hours where both sources have observations.
    Calendar features computed from local (EST) time for analyst usability.
    """
    # Demand side
    demand = (
        spark.read.table("ogp_dev.silver.demand_hourly")
            .select(
                "event_ts_utc",
                "market_demand_mw",
                "ontario_demand_mw",
            )
    )

    # Weather side
    weather = (
        spark.read.table("ogp_dev.silver.weather_hourly")
            .select(
                "event_ts_utc",
                "station_id",
                "station_name",
                "temp_c",
                "dew_point_c",
                "rel_humidity_pct",
                "wind_speed_kmh",
                "wind_dir_10s_deg",
                "precip_mm",
                "stn_pressure_kpa",
                "hdh",
                "cdh",
            )
    )

    # INNER join — only hours where both demand and weather exist
    joined = demand.join(weather, on="event_ts_utc", how="inner")

    return (
        joined
        # Local-time helpers (EST = UTC − 5)
        .withColumn("event_ts_local",     expr("event_ts_utc - INTERVAL '5 HOURS'"))
        .withColumn("event_date_local",   expr("CAST(event_ts_local AS DATE)"))
        .withColumn("hour_of_day_local",  hour("event_ts_local"))
        .withColumn("day_of_week",        dayofweek("event_ts_local"))   # 1=Sun .. 7=Sat
        .withColumn("is_weekend",         expr("dayofweek(event_ts_local) IN (1, 7)"))
        .withColumn("month",              month("event_ts_local"))
        .withColumn("year",               year("event_ts_local"))
        .drop("event_ts_local")    # intermediate column, not exposed

        # Derived demand metric
        .withColumn("net_export_mw",      col("market_demand_mw") - col("ontario_demand_mw"))

        # Temperature buckets for easy categorical analysis
        .withColumn(
            "temp_category",
            when(col("temp_c") < -10, "extreme_cold")
            .when(col("temp_c") <   0, "cold")
            .when(col("temp_c") <  15, "mild")
            .when(col("temp_c") <  25, "warm")
            .when(col("temp_c") <  30, "hot")
            .otherwise("extreme_hot")
        )

        # Audit
        .withColumn("_gold_ts", current_timestamp())
    )