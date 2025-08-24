#!/usr/bin/env python3
"""
ML-Powered Real-Time Log Processing Pipeline
Integrates Kafka, Spark Structured Streaming, ML models, and Elasticsearch
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline
from delta import *
import json
import argparse
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MLLogProcessor:
    def __init__(self, app_name="MLLogProcessor"):
        # Enhanced Spark configuration for ML and streaming
        builder = SparkSession.builder.appName(app_name) \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.adaptive.enabled", "false") \  # Disable for streaming
            .config("spark.sql.streaming.statefulOperator.checkpointing.enabled", "true") \
            .config("spark.sql.streaming.statefulOperator.checkpointing.interval", "10") \
            .config("spark.sql.streaming.statefulOperator.checkpointing.timeout", "60") \
            .config("spark.sql.streaming.minBatchesToRetain", "2") \
            .config("spark.sql.streaming.maxBatchesToRetainInMemory", "10") \
            .config("spark.memory.offHeap.enabled", "true") \
            .config("spark.memory.offHeap.size", "1g")
        
        self.spark = configure_spark_with_delta_pip(builder).getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")
        
        # Initialize ML models
        self.anomaly_model = None
        self.clustering_model = None
        self.classification_model = None
        
        logger.info(f"✅ ML Log Processor initialized with Spark {self.spark.version}")

    def define_enhanced_schema(self):
        """Enhanced schema with additional ML features"""
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

    def create_ml_features(self, df):
        """Create ML features from log data"""
        return df \
            .withColumn("processed_timestamp", current_timestamp()) \
            .withColumn("date", to_date(col("timestamp"))) \
            .withColumn("hour", hour(col("timestamp"))) \
            .withColumn("minute", minute(col("timestamp"))) \
            .withColumn("day_of_week", dayofweek(col("timestamp"))) \
            .withColumn("is_error", col("status_code") >= 400) \
            .withColumn("is_slow", col("response_time") > 1000) \
            .withColumn("is_critical_error", col("status_code") >= 500) \
            .withColumn("ip_class", 
                when(col("ip").startswith("192.168"), "internal")
                .when(col("ip").startswith("10."), "internal")
                .otherwise("external")) \
            .withColumn("endpoint_category",
                when(col("endpoint").startswith("/api/"), "api")
                .when(col("endpoint").startswith("/admin"), "admin")
                .when(col("endpoint").startswith("/auth"), "auth")
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

    def train_anomaly_detection_model(self, training_data_path):
        """Train anomaly detection model using K-means clustering"""
        logger.info("🔬 Training anomaly detection model...")
        
        # Read training data
        training_df = self.spark.read.format("delta").load(training_data_path)
        
        # Create numerical features for ML
        feature_cols = ["response_time", "status_code", "bytes_sent", "request_size"]
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
        
        # Standardize features
        scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
        
        # K-means clustering for anomaly detection
        kmeans = KMeans(k=3, seed=42, featuresCol="scaled_features")
        
        # Create pipeline
        pipeline = Pipeline(stages=[assembler, scaler, kmeans])
        
        # Train model
        self.anomaly_model = pipeline.fit(training_df)
        logger.info("✅ Anomaly detection model trained successfully")
        
        return self.anomaly_model

    def apply_ml_models(self, df):
        """Apply trained ML models to streaming data"""
        if self.anomaly_model is None:
            logger.warning("⚠️ No ML model available, skipping ML inference")
            return df
        
        # Apply anomaly detection
        df_with_features = df \
            .withColumn("features", 
                array(col("response_time"), col("status_code"), 
                      col("bytes_sent"), col("request_size")))
        
        # Make predictions
        predictions = self.anomaly_model.transform(df_with_features)
        
        # Add anomaly scores and labels
        enriched_df = predictions \
            .withColumn("anomaly_score", 
                when(col("prediction") == 0, 0.1)  # Normal cluster
                .when(col("prediction") == 1, 0.5)  # Medium risk
                .otherwise(0.9))  # High risk cluster \
            .withColumn("is_anomaly", col("anomaly_score") > 0.7) \
            .withColumn("ml_confidence", 
                when(col("anomaly_score") > 0.8, "high")
                .when(col("anomaly_score") > 0.5, "medium")
                .otherwise("low"))
        
        return enriched_df

    def start_ml_streaming(self, kafka_servers="localhost:9092", topic="web-logs", 
                          output_path="/tmp/delta-lake/ml-enriched-logs",
                          checkpoint_path="/tmp/checkpoints/ml-logs",
                          elasticsearch_host="localhost:9200"):
        """Start ML-powered streaming processing"""
        
        logger.info(f"🚀 Starting ML-powered streaming from Kafka topic: {topic}")
        logger.info(f"📊 Output Delta Lake: {output_path}")
        logger.info(f"🔍 Elasticsearch: {elasticsearch_host}")
        
        # Define schema
        log_schema = self.define_enhanced_schema()
        
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
        
        # Create ML features
        enriched_df = self.create_ml_features(parsed_df)
        
        # Apply ML models if available
        ml_enriched_df = self.apply_ml_models(enriched_df)
        
        # Add metadata for Elasticsearch
        final_df = ml_enriched_df \
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
        
        logger.info("✅ ML streaming pipeline started!")
        
        try:
            # Wait for termination
            delta_query.awaitTermination()
        except KeyboardInterrupt:
            logger.info("🛑 Stopping ML streaming pipeline...")
            delta_query.stop()
            console_query.stop()
            if elasticsearch_host != "none":
                es_query.stop()
            logger.info("✅ ML streaming pipeline stopped")

    def run_ml_analytics(self, ml_logs_path="/tmp/delta-lake/ml-enriched-logs"):
        """Run ML analytics on enriched logs"""
        logger.info(f"📈 Running ML analytics on: {ml_logs_path}")
        
        # Read enriched logs
        logs_df = self.spark.read.format("delta").load(ml_logs_path)
        logs_df.createOrReplaceTempView("ml_logs")
        
        # ML Analytics queries
        logger.info("\n=== ANOMALY DETECTION RESULTS ===")
        anomaly_summary = self.spark.sql("""
            SELECT 
                anomaly_type,
                COUNT(*) as count,
                AVG(anomaly_score) as avg_score,
                COUNT(DISTINCT endpoint) as affected_endpoints
            FROM (
                SELECT 
                    CASE 
                        WHEN is_anomaly THEN 'ANOMALY'
                        WHEN is_error THEN 'ERROR'
                        ELSE 'NORMAL'
                    END as anomaly_type,
                    anomaly_score,
                    endpoint
                FROM ml_logs
                WHERE date >= current_date() - 1
            )
            GROUP BY anomaly_type
            ORDER BY count DESC
        """)
        anomaly_summary.show()
        
        logger.info("\n=== ML MODEL PERFORMANCE ===")
        model_performance = self.spark.sql("""
            SELECT 
                ml_confidence,
                COUNT(*) as predictions,
                AVG(anomaly_score) as avg_score,
                SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END) as anomalies_detected
            FROM ml_logs
            WHERE date >= current_date() - 1
            GROUP BY ml_confidence
            ORDER BY predictions DESC
        """)
        model_performance.show()
        
        logger.info("\n=== TRAFFIC PATTERNS BY TIME ===")
        time_patterns = self.spark.sql("""
            SELECT 
                hour,
                COUNT(*) as total_requests,
                AVG(anomaly_score) as avg_anomaly_score,
                SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END) as anomalies
            FROM ml_logs
            WHERE date >= current_date() - 1
            GROUP BY hour
            ORDER BY hour
        """)
        time_patterns.show()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='ML-powered real-time log processing')
    parser.add_argument('--mode', choices=['stream', 'analytics', 'train'], 
                       default='stream', help='Execution mode')
    parser.add_argument('--kafka-servers', default='localhost:9092', help='Kafka bootstrap servers')
    parser.add_argument('--topic', default='web-logs', help='Kafka topic')
    parser.add_argument('--output-path', default='/tmp/delta-lake/ml-enriched-logs', help='Delta Lake output path')
    parser.add_argument('--checkpoint-path', default='/tmp/checkpoints/ml-logs', help='Checkpoint location')
    parser.add_argument('--elasticsearch-host', default='localhost:9200', help='Elasticsearch host')
    parser.add_argument('--training-data', default='/tmp/delta-lake/logs', help='Training data path')
    
    args = parser.parse_args()
    
    processor = MLLogProcessor()
    
    if args.mode == 'stream':
        processor.start_ml_streaming(
            kafka_servers=args.kafka_servers,
            topic=args.topic,
            output_path=args.output_path,
            checkpoint_path=args.checkpoint_path,
            elasticsearch_host=args.elasticsearch_host
        )
    elif args.mode == 'analytics':
        processor.run_ml_analytics(args.output_path)
    elif args.mode == 'train':
        processor.train_anomaly_detection_model(args.training_data)