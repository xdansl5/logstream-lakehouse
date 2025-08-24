#!/usr/bin/env python3
"""
Simple Streaming Processor for LogStream Lakehouse
Rule-based anomaly detection without ML dependencies
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta import *
import json
import argparse
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SimpleStreamingProcessor:
    def __init__(self, app_name="SimpleStreamingProcessor"):
        # Basic Spark configuration for streaming
        builder = SparkSession.builder.appName(app_name) \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.adaptive.enabled", "false") \
            .config("spark.sql.streaming.statefulOperator.checkpointing.enabled", "true") \
            .config("spark.sql.streaming.statefulOperator.checkpointing.interval", "10") \
            .config("spark.sql.streaming.statefulOperator.checkpointing.timeout", "60")
        
        self.spark = configure_spark_with_delta_pip(builder).getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")
        
        logger.info(f"✅ Simple Streaming Processor initialized with Spark {self.spark.version}")

    def define_schema(self):
        """Define schema for log data"""
        return StructType([
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

    def enrich_logs(self, df):
        """Enrich logs with additional features and anomaly detection"""
        return df \
            .withColumn("processed_timestamp", current_timestamp()) \
            .withColumn("date", to_date(col("timestamp"))) \
            .withColumn("hour", hour(col("timestamp"))) \
            .withColumn("minute", minute(col("timestamp"))) \
            .withColumn("day_of_week", dayofweek(col("timestamp"))) \
            .withColumn("is_error", col("status_code") >= 400) \
            .withColumn("is_critical_error", col("status_code") >= 500) \
            .withColumn("is_slow", col("response_time") > 1000) \
            .withColumn("is_very_slow", col("response_time") > 3000) \
            .withColumn("ip_class", 
                when(col("ip").startswith("192.168"), "internal")
                .when(col("ip").startswith("10."), "internal")
                .when(col("ip").startswith("172.16"), "internal")
                .otherwise("external")) \
            .withColumn("endpoint_category",
                when(col("endpoint").startswith("/api/"), "api")
                .when(col("endpoint").startswith("/admin"), "admin")
                .when(col("endpoint").startswith("/auth"), "auth")
                .when(col("endpoint").startswith("/health"), "health")
                .otherwise("web")) \
            .withColumn("method_category",
                when(col("method").isin(["GET", "HEAD"]), "read")
                .when(col("method").isin(["POST", "PUT", "PATCH"]), "write")
                .when(col("method").isin(["DELETE"]), "delete")
                .otherwise("other")) \
            .withColumn("response_time_category",
                when(col("response_time") < 100, "fast")
                .when(col("response_time") < 500, "medium")
                .when(col("response_time") < 1000, "slow")
                .otherwise("very_slow")) \
            .withColumn("request_size_category",
                when(col("request_size") < 1024, "small")
                .when(col("request_size") < 10240, "medium")
                .otherwise("large")) \
            .withColumn("hour_traffic_peak", 
                when(col("hour").between(9, 17), "business_hours")
                .when(col("hour").between(18, 22), "evening")
                .otherwise("off_hours"))

    def detect_anomalies(self, df):
        """Apply rule-based anomaly detection"""
        logger.info("🔍 Applying rule-based anomaly detection...")
        
        return df \
            .withColumn("anomaly_score", 
                when(col("is_critical_error"), 0.9)  # Critical errors
                .when(col("is_very_slow"), 0.85)  # Very slow responses
                .when(col("is_slow"), 0.7)  # Slow responses
                .when(col("is_error"), 0.6)  # Regular errors
                .when(col("response_time") > 500, 0.4)  # Medium response times
                .when(col("status_code") == 404, 0.3)  # Not found errors
                .otherwise(0.1))  # Normal \
            .withColumn("is_anomaly", col("anomaly_score") > 0.5) \
            .withColumn("anomaly_type",
                when(col("is_critical_error"), "CRITICAL_ERROR")
                .when(col("is_very_slow"), "VERY_SLOW_RESPONSE")
                .when(col("is_slow"), "SLOW_RESPONSE")
                .when(col("is_error"), "ERROR")
                .when(col("response_time") > 500, "MEDIUM_RESPONSE")
                .when(col("status_code") == 404, "NOT_FOUND")
                .otherwise("NORMAL")) \
            .withColumn("severity",
                when(col("anomaly_score") >= 0.8, "HIGH")
                .when(col("anomaly_score") >= 0.5, "MEDIUM")
                .otherwise("LOW")) \
            .withColumn("needs_attention",
                when(col("anomaly_score") >= 0.7, True)
                .otherwise(False))

    def start_streaming(self, kafka_servers="localhost:9092", topic="web-logs", 
                       output_path="/tmp/delta-lake/enriched-logs",
                       checkpoint_path="/tmp/checkpoints/enriched-logs",
                       elasticsearch_host="localhost:9200"):
        """Start streaming processing"""
        
        logger.info(f"🚀 Starting streaming from Kafka topic: {topic}")
        logger.info(f"📊 Output Delta Lake: {output_path}")
        logger.info(f"🔍 Elasticsearch: {elasticsearch_host}")
        
        # Define schema
        log_schema = self.define_schema()
        
        # Read from Kafka
        kafka_df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_servers) \
            .option("subscribe", topic) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .option("maxOffsetsPerTrigger", 1000) \
            .load()
        
        # Parse JSON and apply transformations
        parsed_df = kafka_df \
            .select(from_json(col("value").cast("string"), log_schema).alias("data")) \
            .select("data.*") \
            .withColumn("timestamp", to_timestamp(col("timestamp"))) \
            .filter(col("timestamp").isNotNull())
        
        # Enrich logs
        enriched_df = self.enrich_logs(parsed_df)
        
        # Detect anomalies
        final_df = self.detect_anomalies(enriched_df)
        
        # Add metadata for Elasticsearch
        final_df = final_df \
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
                      col("ip_class"), col("severity")))
        
        # Write to Delta Lake
        delta_query = final_df \
            .writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("checkpointLocation", checkpoint_path) \
            .partitionBy("date", "hour") \
            .option("path", output_path) \
            .trigger(processingTime='10 seconds') \
            .start()
        
        # Write to Elasticsearch (if configured)
        if elasticsearch_host != "none":
            es_query = final_df \
                .writeStream \
                .format("org.elasticsearch.spark.sql") \
                .option("es.nodes", elasticsearch_host) \
                .option("es.index.auto.create", "true") \
                .option("es.mapping.id", "_id") \
                .option("es.write.operation", "index") \
                .option("es.batch.size.entries", "100") \
                .option("es.batch.size.bytes", "5mb") \
                .option("es.batch.write.refresh", "false") \
                .option("checkpointLocation", f"{checkpoint_path}/elasticsearch") \
                .trigger(processingTime='10 seconds') \
                .start()
        
        # Console output for monitoring
        console_query = final_df \
            .writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", False) \
            .option("numRows", 20) \
            .trigger(processingTime='30 seconds') \
            .start()
        
        logger.info("✅ Streaming pipeline started!")
        
        try:
            # Wait for termination
            delta_query.awaitTermination()
        except KeyboardInterrupt:
            logger.info("🛑 Stopping streaming pipeline...")
            delta_query.stop()
            console_query.stop()
            if elasticsearch_host != "none":
                es_query.stop()
            logger.info("✅ Streaming pipeline stopped")

    def run_analytics(self, logs_path="/tmp/delta-lake/enriched-logs"):
        """Run analytics on the processed data"""
        logger.info(f"📈 Running analytics on: {logs_path}")
        
        # Read enriched logs
        logs_df = self.spark.read.format("delta").load(logs_path)
        logs_df.createOrReplaceTempView("enriched_logs")
        
        # Analytics queries
        logger.info("\n=== ANOMALY DETECTION SUMMARY ===")
        anomaly_summary = self.spark.sql("""
            SELECT 
                anomaly_type,
                COUNT(*) as count,
                AVG(anomaly_score) as avg_score,
                COUNT(DISTINCT endpoint) as affected_endpoints
            FROM enriched_logs
            WHERE date >= current_date() - 1
            GROUP BY anomaly_type
            ORDER BY count DESC
        """)
        anomaly_summary.show()
        
        logger.info("\n=== SEVERITY DISTRIBUTION ===")
        severity_dist = self.spark.sql("""
            SELECT 
                severity,
                COUNT(*) as count,
                AVG(anomaly_score) as avg_score
            FROM enriched_logs
            WHERE date >= current_date() - 1
            GROUP BY severity
            ORDER BY count DESC
        """)
        severity_dist.show()
        
        logger.info("\n=== TRAFFIC PATTERNS BY TIME ===")
        time_patterns = self.spark.sql("""
            SELECT 
                hour,
                COUNT(*) as total_requests,
                AVG(anomaly_score) as avg_anomaly_score,
                SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END) as anomalies,
                SUM(CASE WHEN needs_attention THEN 1 ELSE 0 END) as needs_attention
            FROM enriched_logs
            WHERE date >= current_date() - 1
            GROUP BY hour
            ORDER BY hour
        """)
        time_patterns.show()
        
        logger.info("\n=== TOP ENDPOINTS WITH ISSUES ===")
        top_issues = self.spark.sql("""
            SELECT 
                endpoint,
                COUNT(*) as total_requests,
                SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END) as anomalies,
                AVG(anomaly_score) as avg_score,
                AVG(response_time) as avg_response_time
            FROM enriched_logs
            WHERE date >= current_date() - 1
            GROUP BY endpoint
            HAVING COUNT(*) > 10
            ORDER BY anomalies DESC
            LIMIT 10
        """)
        top_issues.show()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Simple streaming processor for logs')
    parser.add_argument('--mode', choices=['stream', 'analytics'], 
                       default='stream', help='Execution mode')
    parser.add_argument('--kafka-servers', default='localhost:9092', help='Kafka bootstrap servers')
    parser.add_argument('--topic', default='web-logs', help='Kafka topic')
    parser.add_argument('--output-path', default='/tmp/delta-lake/enriched-logs', help='Delta Lake output path')
    parser.add_argument('--checkpoint-path', default='/tmp/checkpoints/enriched-logs', help='Checkpoint location')
    parser.add_argument('--elasticsearch-host', default='localhost:9200', help='Elasticsearch host')
    
    args = parser.parse_args()
    
    processor = SimpleStreamingProcessor()
    
    if args.mode == 'stream':
        processor.start_streaming(
            kafka_servers=args.kafka_servers,
            topic=args.topic,
            output_path=args.output_path,
            checkpoint_path=args.checkpoint_path,
            elasticsearch_host=args.elasticsearch_host
        )
    elif args.mode == 'analytics':
        processor.run_analytics(args.output_path)