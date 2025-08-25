#!/usr/bin/env python3
"""
ML-Powered Real-Time Log Processing Pipeline
Integrates Kafka, Spark Structured Streaming, ML models, and Elasticsearch
"""

import argparse
import json
import logging
from datetime import datetime

from delta import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Try to import ML libraries with compatibility handling
try:
    from pyspark.ml import Pipeline
    from pyspark.ml.clustering import KMeans
    from pyspark.ml.classification import RandomForestClassifier
    from pyspark.ml.feature import VectorAssembler, StandardScaler

    ML_AVAILABLE = True
    logger.info("✅ PySpark ML libraries loaded successfully")
except ImportError as e:
    logger.warning(f"⚠️ PySpark ML libraries not available: {e}")
    logger.warning("⚠️ ML features will be disabled. Using rule-based anomaly detection instead.")
    ML_AVAILABLE = False


class MLLogProcessor:
    """Class for ML-powered log processing with Spark, Kafka, and Elasticsearch."""

    def __init__(self, app_name="MLLogProcessor"):
        # Enhanced Spark configuration for ML and streaming
        builder = (
            SparkSession.builder.appName(app_name)
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.sql.adaptive.enabled", "false")
            .config("spark.sql.streaming.statefulOperator.checkpointing.enabled", "true")
            .config("spark.sql.streaming.statefulOperator.checkpointing.interval", "10")
            .config("spark.sql.streaming.statefulOperator.checkpointing.timeout", "60")
            .config("spark.sql.streaming.minBatchesToRetain", "2")
            .config("spark.sql.streaming.maxBatchesToRetainInMemory", "10")
            .config("spark.memory.offHeap.enabled", "true")
            .config("spark.memory.offHeap.size", "1g")
        )

        self.spark = configure_spark_with_delta_pip(builder).getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")

        # Initialize ML models
        self.anomaly_model = None
        self.clustering_model = None
        self.classification_model = None

        logger.info(f"✅ ML Log Processor initialized with Spark {self.spark.version}")
        logger.info(f"🤖 ML capabilities: {'Available' if ML_AVAILABLE else 'Disabled'}")

    # ---------------------------------------------------------
    # SCHEMA & FEATURE ENGINEERING
    # ---------------------------------------------------------

    def define_enhanced_schema(self):
        """Enhanced schema with additional ML features."""
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
            StructField("query_params", StringType(), True),
        ])

    def create_ml_features(self, df):
        """Create ML features from log data."""
        return (
            df.withColumn("processed_timestamp", current_timestamp())
              .withColumn("date", to_date(col("timestamp")))
              .withColumn("hour", hour(col("timestamp")))
              .withColumn("minute", minute(col("timestamp")))
              .withColumn("day_of_week", dayofweek(col("timestamp")))
              .withColumn("is_error", col("status_code") >= 400)
              .withColumn("is_slow", col("response_time") > 1000)
              .withColumn("is_critical_error", col("status_code") >= 500)
              .withColumn(
                  "ip_class",
                  when(col("ip").startswith("192.168"), "internal")
                  .when(col("ip").startswith("10."), "internal")
                  .otherwise("external"),
              )
              .withColumn(
                  "endpoint_category",
                  when(col("endpoint").startswith("/api/"), "api")
                  .when(col("endpoint").startswith("/admin"), "admin")
                  .when(col("endpoint").startswith("/auth"), "auth")
                  .otherwise("web"),
              )
              .withColumn(
                  "method_category",
                  when(col("method").isin(["GET", "HEAD"]), "read")
                  .when(col("method").isin(["POST", "PUT", "PATCH"]), "write")
                  .when(col("method").isin(["DELETE"]), "delete")
                  .otherwise("other"),
              )
              .withColumn(
                  "response_time_category",
                  when(col("response_time") < 100, "fast")
                  .when(col("response_time") < 500, "medium")
                  .when(col("response_time") < 1000, "slow")
                  .otherwise("very_slow"),
              )
              .withColumn(
                  "hour_traffic_peak",
                  when(col("hour").between(9, 17), "business_hours")
                  .when(col("hour").between(18, 22), "evening")
                  .otherwise("off_hours"),
              )
        )

    # ---------------------------------------------------------
    # RULE-BASED DETECTION
    # ---------------------------------------------------------

    def apply_rule_based_anomaly_detection(self, df):
        """Apply rule-based anomaly detection when ML is not available."""
        logger.info("🔍 Applying rule-based anomaly detection...")

        return (
            df.withColumn(
                "anomaly_score",
                when(col("is_critical_error"), 0.9)
                .when(col("is_slow") & (col("response_time") > 2000), 0.8)
                .when(col("is_slow"), 0.6)
                .when(col("is_error"), 0.5)
                .when(col("response_time") > 500, 0.3)
                .otherwise(0.1),
            )
            .withColumn("is_anomaly", col("anomaly_score") > 0.5)
            .withColumn(
                "ml_confidence",
                when(col("anomaly_score") > 0.8, "high")
                .when(col("anomaly_score") > 0.5, "medium")
                .otherwise("low"),
            )
            .withColumn(
                "anomaly_type",
                when(col("is_critical_error"), "CRITICAL_ERROR")
                .when(col("is_slow") & (col("response_time") > 2000), "VERY_SLOW_RESPONSE")
                .when(col("is_slow"), "SLOW_RESPONSE")
                .when(col("is_error"), "ERROR")
                .when(col("response_time") > 500, "MEDIUM_RESPONSE")
                .otherwise("NORMAL"),
            )
        )

    # ---------------------------------------------------------
    # TRAINING & INFERENCE
    # ---------------------------------------------------------

    def train_anomaly_detection_model(self, training_data_path):
        """Train anomaly detection model using K-means clustering."""
        if not ML_AVAILABLE:
            logger.warning("⚠️ ML libraries not available, skipping model training")
            return None

        logger.info("🔬 Training anomaly detection model...")

        try:
            # Read training data
            training_df = self.spark.read.format("delta").load(training_data_path)

            # Create numerical features for ML
            feature_cols = ["response_time", "status_code", "bytes_sent"]
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

        except Exception as e:
            logger.error(f"❌ ML model training failed: {e}")
            logger.info("🔄 Falling back to rule-based detection")
            return None

    def apply_ml_models(self, df):
        """Apply trained ML models to streaming data."""
        if self.anomaly_model is None or not ML_AVAILABLE:
            logger.info("🔄 Using rule-based anomaly detection")
            return self.apply_rule_based_anomaly_detection(df)

        try:
            df_with_features = df.withColumn(
                "features",
                array(col("response_time"), col("status_code"), col("bytes_sent")),
            )

            # Make predictions
            predictions = self.anomaly_model.transform(df_with_features)

            return (
                predictions.withColumn(
                    "anomaly_score",
                    when(col("prediction") == 0, 0.1)
                    .when(col("prediction") == 1, 0.5)
                    .otherwise(0.9),
                )
                .withColumn("is_anomaly", col("anomaly_score") > 0.7)
                .withColumn(
                    "ml_confidence",
                    when(col("anomaly_score") > 0.8, "high")
                    .when(col("anomaly_score") > 0.5, "medium")
                    .otherwise("low"),
                )
                .withColumn(
                    "anomaly_type",
                    when(col("prediction") == 0, "NORMAL")
                    .when(col("prediction") == 1, "MEDIUM_RISK")
                    .otherwise("HIGH_RISK"),
                )
            )

        except Exception as e:
            logger.error(f"❌ ML inference failed: {e}, falling back to rule-based detection")
            return self.apply_rule_based_anomaly_detection(df)

    # ---------------------------------------------------------
    # STREAMING PIPELINE
    # ---------------------------------------------------------

    def start_ml_streaming(
        self,
        kafka_servers="localhost:9092",
        topic="web-logs",
        output_path="/tmp/delta-lake/ml-enriched-logs",
        checkpoint_path="/tmp/checkpoints/ml-logs",
        elasticsearch_host="localhost:9200",
    ):
        """Start ML-powered streaming processing."""
        logger.info(f"🚀 Starting ML-powered streaming from Kafka topic: {topic}")
        logger.info(f"📊 Output Delta Lake: {output_path}")
        logger.info(f"🔍 Elasticsearch: {elasticsearch_host}")
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
        
        # Debug: Log the columns after ML processing
        logger.info(f"🔍 Columns after ML processing: {ml_enriched_df.columns}")
        
        # Ensure all required columns exist for downstream processing
        final_df = ml_enriched_df \
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
        
        # Debug: Log the columns after ensuring required columns
        logger.info(f"🔍 Columns after ensuring required columns: {final_df.columns}")
        
        # Debug: Show sample data
        logger.info("🔍 Sample data structure:")
        (final_df
        .select("timestamp", "ip", "endpoint", "is_anomaly", "anomaly_score", "ml_confidence", "anomaly_type")
        .writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", False)
        .start()
        .awaitTermination()
    )

        
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