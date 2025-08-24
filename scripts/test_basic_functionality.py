#!/usr/bin/env python3
"""
Test Basic Functionality
Verifies that Spark and Delta Lake work without ML dependencies
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta import *
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_basic_spark():
    """Test basic Spark functionality"""
    logger.info("🧪 Testing basic Spark functionality...")
    
    try:
        # Create Spark session
        builder = SparkSession.builder.appName("BasicFunctionalityTest") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        
        logger.info(f"✅ Spark session created successfully: {spark.version}")
        
        # Test basic DataFrame operations
        data = [
            ("2024-08-25T23:00:00", "192.168.1.1", "GET", "/api/test", 200, 150),
            ("2024-08-25T23:01:00", "192.168.1.2", "POST", "/api/test", 201, 300),
            ("2024-08-25T23:02:00", "10.0.0.1", "GET", "/health", 200, 50)
        ]
        
        schema = StructType([
            StructField("timestamp", StringType(), True),
            StructField("ip", StringType(), True),
            StructField("method", StringType(), True),
            StructField("endpoint", StringType(), True),
            StructField("status_code", IntegerType(), True),
            StructField("response_time", IntegerType(), True)
        ])
        
        df = spark.createDataFrame(data, schema)
        logger.info(f"✅ DataFrame created successfully with {df.count()} rows")
        
        # Test basic transformations
        enriched_df = df \
            .withColumn("timestamp_ts", to_timestamp(col("timestamp"))) \
            .withColumn("date", to_date(col("timestamp"))) \
            .withColumn("hour", hour(col("timestamp"))) \
            .withColumn("is_error", col("status_code") >= 400) \
            .withColumn("ip_class", 
                when(col("ip").startswith("192.168"), "internal")
                .otherwise("external"))
        
        logger.info("✅ DataFrame transformations completed successfully")
        
        # Test SQL operations
        enriched_df.createOrReplaceTempView("test_logs")
        
        result = spark.sql("""
            SELECT 
                date,
                COUNT(*) as total_requests,
                AVG(response_time) as avg_response_time,
                SUM(CASE WHEN is_error THEN 1 ELSE 0 END) as error_count
            FROM test_logs
            GROUP BY date
        """)
        
        logger.info("✅ SQL operations completed successfully")
        result.show()
        
        # Test Delta Lake operations
        test_path = "/tmp/test-delta-lake"
        
        # Write to Delta Lake
        enriched_df.write.format("delta").mode("overwrite").save(test_path)
        logger.info(f"✅ Data written to Delta Lake: {test_path}")
        
        # Read from Delta Lake
        read_df = spark.read.format("delta").load(test_path)
        logger.info(f"✅ Data read from Delta Lake: {read_df.count()} rows")
        
        # Cleanup
        import shutil
        shutil.rmtree(test_path, ignore_errors=True)
        logger.info("✅ Test cleanup completed")
        
        spark.stop()
        logger.info("✅ Basic Spark functionality test PASSED")
        return True
        
    except Exception as e:
        logger.error(f"❌ Basic Spark functionality test FAILED: {e}")
        return False

def test_streaming_operations():
    """Test basic streaming operations"""
    logger.info("🧪 Testing basic streaming operations...")
    
    try:
        # Create Spark session
        builder = SparkSession.builder.appName("StreamingTest") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.sql.streaming.statefulOperator.checkpointing.enabled", "true")
        
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        
        logger.info("✅ Streaming Spark session created successfully")
        
        # Test streaming DataFrame creation (without actually starting streaming)
        data = [
            ("2024-08-25T23:00:00", "192.168.1.1", "GET", "/api/test", 200, 150),
            ("2024-08-25T23:01:00", "192.168.1.2", "POST", "/api/test", 201, 300)
        ]
        
        schema = StructType([
            StructField("timestamp", StringType(), True),
            StructField("ip", StringType(), True),
            StructField("method", StringType(), True),
            StructField("endpoint", StringType(), True),
            StructField("status_code", IntegerType(), True),
            StructField("response_time", IntegerType(), True)
        ])
        
        df = spark.createDataFrame(data, schema)
        
        # Test streaming-like operations
        enriched_df = df \
            .withColumn("timestamp_ts", to_timestamp(col("timestamp"))) \
            .withColumn("is_error", col("status_code") >= 400) \
            .withColumn("anomaly_score", 
                when(col("is_error"), 0.8)
                .when(col("response_time") > 200, 0.5)
                .otherwise(0.1))
        
        logger.info("✅ Streaming-like operations completed successfully")
        
        # Test windowing functions (these work in batch mode)
        windowed_df = enriched_df \
            .withColumn("hour", hour(col("timestamp_ts"))) \
            .groupBy("hour") \
            .agg(
                count("*").alias("request_count"),
                avg("response_time").alias("avg_response_time"),
                avg("anomaly_score").alias("avg_anomaly_score")
            )
        
        logger.info("✅ Windowing operations completed successfully")
        windowed_df.show()
        
        spark.stop()
        logger.info("✅ Streaming operations test PASSED")
        return True
        
    except Exception as e:
        logger.error(f"❌ Streaming operations test FAILED: {e}")
        return False

def main():
    """Run all tests"""
    logger.info("🚀 Starting basic functionality tests...")
    
    tests = [
        ("Basic Spark", test_basic_spark),
        ("Streaming Operations", test_streaming_operations)
    ]
    
    results = []
    for test_name, test_func in tests:
        logger.info(f"\n{'='*50}")
        logger.info(f"Running test: {test_name}")
        logger.info(f"{'='*50}")
        
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            logger.error(f"Test {test_name} failed with exception: {e}")
            results.append((test_name, False))
    
    # Summary
    logger.info(f"\n{'='*50}")
    logger.info("TEST SUMMARY")
    logger.info(f"{'='*50}")
    
    passed = 0
    total = len(results)
    
    for test_name, result in results:
        status = "✅ PASSED" if result else "❌ FAILED"
        logger.info(f"{test_name}: {status}")
        if result:
            passed += 1
    
    logger.info(f"\nOverall: {passed}/{total} tests passed")
    
    if passed == total:
        logger.info("🎉 All tests passed! Basic functionality is working correctly.")
        return True
    else:
        logger.error("💥 Some tests failed. Please check the errors above.")
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)