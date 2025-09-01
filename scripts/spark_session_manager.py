#!/usr/bin/env python3
"""
Shared Spark session manager to enforce a single SparkSession per process.
All jobs should import `get_spark` to obtain the configured session.
"""

from typing import Optional
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

_spark_singleton: Optional[SparkSession] = None


def get_spark(app_name: str = "LogStreamApp") -> SparkSession:
    """Create or return a singleton SparkSession configured for Delta Lake.

    This function ensures only one SparkContext exists in the JVM, avoiding
    SPARK-2243 errors by reusing the same session instance.
    """
    global _spark_singleton
    if _spark_singleton is not None:
        return _spark_singleton

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    )

    _spark_singleton = configure_spark_with_delta_pip(builder).getOrCreate()
    _spark_singleton.sparkContext.setLogLevel("WARN")
    return _spark_singleton


def stop_spark():
    """Stop the singleton SparkSession if it exists."""
    global _spark_singleton
    if _spark_singleton is not None:
        try:
            _spark_singleton.stop()
        finally:
            _spark_singleton = None

