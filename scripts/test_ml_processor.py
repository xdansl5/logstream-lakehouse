#!/usr/bin/env python3
"""
Test ML Processor
Tests the ML processor functionality without starting streaming
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta import *
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_ml_processor():
    """Test the ML processor functionality"""
    logger.info("🧪 Testing ML processor functionality...")
    
    try:
        # Create Spark session
        builder = SparkSession.builder.appName("MLProcessorTest") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        
        logger.info(f"✅ Spark session created successfully: {spark.version}")
        
        # Create test data
        test_data = [
            ("2024-08-25T00:00:00", "192.168.1.1", "GET", "/api/test", 200, 150, "Mozilla/5.0", "user1", "session1", 1024, "https://google.com", 512, "?param=1"),
            ("2024-08-25T00:01:00", "192.168.1.2", "POST", "/api/test", 500, 2500, "Mozilla/5.0", "user2", "session2", 2048, "https://bing.com", 1024, "?param=2"),
            ("2024-08-25T00:02:00", "10.0.0.1", "GET", "/health", 200, 50, "Mozilla/5.0", "user3", "session3", 512, None, 256, ""),
            ("2024-08-25T00:03:00", "203.0.113.1", "GET", "/api/users", 404, 300, "Mozilla/5.0", "user4", "session4", 1024, "https://example.com", 512, "?page=1"),
            ("2024-08-25T00:04:00", "198.51.100.1", "POST", "/api/auth", 401, 800, "Mozilla/5.0", "user5", "session5", 1536, "https://auth.com", 768, "?login=user")
        ]
        
        schema = StructType([
            StructField("timestamp", StringType(), True),
            StructField("ip", StringType(), True),
            StructField("method", StringType(), True),
            StructField("endpoint", StringType(), True),
            StructField("status_code", IntegerType(), True),
            StructField("response_time", IntegerType(), True),
            StructField("user_agent", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("session_id", StringType(), True),
            StructField("bytes_sent", IntegerType(), True),
            StructField("referer", StringType(), True),
            StructField("request_size", IntegerType(), True),
            StructField("query_params", StringType(), True)
        ])
        
        df = spark.createDataFrame(test_data, schema)
        logger.info(f"✅ Test DataFrame created with {df.count()} rows")
        
        # Test the ML features creation
        from ml_streaming_processor import MLLogProcessor
        
        processor = MLLogProcessor("MLProcessorTest")
        
        # Test feature creation
        enriched_df = processor.create_ml_features(df)
        logger.info(f"✅ Features created. Columns: {enriched_df.columns}")
        
        # Test anomaly detection
        anomaly_df = processor.apply_ml_models(enriched_df)
        logger.info(f"✅ Anomaly detection applied. Columns: {anomaly_df.columns}")
        
        # Show results
        logger.info("🔍 Anomaly detection results:")
        anomaly_df.select("timestamp", "ip", "endpoint", "status_code", "response_time", 
                         "is_error", "is_slow", "is_critical_error", "anomaly_score", 
                         "is_anomaly", "ml_confidence", "anomaly_type").show(truncate=False)
        
        # Test the column creation logic
        logger.info("🔍 Testing column creation logic...")
        
        # Ensure all required columns exist
        final_df = anomaly_df \
            .withColumn("is_anomaly", 
                when(col("is_anomaly").isNotNull(), col("is_anomaly"))
                .otherwise(lit(False))) \
            .withColumn("anomaly_score", 
                when(col("anomaly_score").isNotNull(), col("anomaly_score"))
                .otherwise(lit(0.1))) \
            .withColumn("ml_confidence", 
                when(col("ml_confidence").isNotNull(), col("ml_confidence"))
                .otherwise(lit("low"))) \
            .withColumn("anomaly_type", 
                when(col("anomaly_type").isNotNull(), col("anomaly_type"))
                .otherwise(lit("NORMAL")))
        
        logger.info(f"✅ Final DataFrame columns: {final_df.columns}")
        
        # Test the metadata creation
        metadata_df = final_df \
            .withColumn("_id", 
                concat(col("timestamp").cast("string"), lit("_"), 
                       col("ip"), lit("_"), col("endpoint"))) \
            .withColumn("@timestamp", col("timestamp")) \
            .withColumn("log_level", 
                when(col("is_anomaly"), "WARN")
                .when(col("is_error"), "ERROR")
                .otherwise("INFO")) \
            .withColumn("tags", 
                array(col("endpoint_category"), col("method_category"), 
                      col("ip_class"), col("ml_confidence")))
        
        logger.info("✅ Metadata creation completed successfully")
        logger.info(f"🔍 Final metadata columns: {metadata_df.columns}")
        
        # Show final results
        logger.info("🔍 Final processed data:")
        metadata_df.select("_id", "log_level", "is_anomaly", "anomaly_score", 
                          "ml_confidence", "anomaly_type", "tags").show(truncate=False)
        
        spark.stop()
        logger.info("✅ ML processor test PASSED")
        return True
        
    except Exception as e:
        logger.error(f"❌ ML processor test FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Run the test"""
    logger.info("🚀 Starting ML processor test...")
    
    success = test_ml_processor()
    
    if success:
        logger.info("🎉 ML processor test completed successfully!")
        logger.info("✅ The ML processor is working correctly.")
        logger.info("✅ You can now use it in streaming mode.")
    else:
        logger.error("💥 ML processor test failed!")
        logger.error("❌ Please check the error messages above.")
    
    return success

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)