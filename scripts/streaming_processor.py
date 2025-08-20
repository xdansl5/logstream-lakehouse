#!/usr/bin/env python3
"""
Spark Structured Streaming Processor
Legge log da Kafka, li processa e li scrive su Delta Lake
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta import *
import argparse

class LogStreamProcessor:
    def __init__(self, app_name="LogStreamProcessor"):
        # Configurazione Spark con Delta Lake
        builder = SparkSession.builder.appName(app_name) \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        
        self.spark = configure_spark_with_delta_pip(builder).getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")
        
        print(f"✅ Spark Session inizializzata: {self.spark.version}")

    def define_schema(self):
        """Definisce lo schema per i log JSON provenienti da Kafka"""
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
            StructField("referer", StringType(), True)
        ])

    def enrich_logs(self, df):
        """Arricchisce i log con informazioni aggiuntive"""
        enriched_df = df \
            .withColumn("processed_timestamp", current_timestamp()) \
            .withColumn("date", to_date(col("timestamp"))) \
            .withColumn("hour", hour(col("timestamp"))) \
            .withColumn("is_error", col("status_code") >= 400) \
            .withColumn("is_slow", col("response_time") > 1000) \
            .withColumn("ip_class", 
                when(col("ip").startswith("192.168"), "internal")
                .when(col("ip").startswith("10."), "internal")
                .otherwise("external")) \
            .withColumn("endpoint_category",
                when(col("endpoint").startswith("/api/"), "api")
                .when(col("endpoint").startswith("/admin"), "admin")
                .otherwise("web"))
        
        return enriched_df

    def start_streaming(self, kafka_servers="localhost:9092", topic="web-logs", 
                       output_path="/tmp/delta-lake/logs", checkpoint_path="/tmp/checkpoints/logs"):
        """Avvia il processing streaming da Kafka a Delta Lake"""
        
        print(f"🚀 Avvio streaming da Kafka topic: {topic}")
        print(f"📊 Output Delta Lake: {output_path}")
        
        # Schema per i dati JSON
        log_schema = self.define_schema()
        
        # Legge da Kafka
        kafka_df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_servers) \
            .option("subscribe", topic) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        # Parse del JSON e trasformazioni
        parsed_df = kafka_df \
            .select(from_json(col("value").cast("string"), log_schema).alias("data")) \
            .select("data.*") \
            .withColumn("timestamp", to_timestamp(col("timestamp"))) \
            .filter(col("timestamp").isNotNull())
        
        # Arricchimento dei dati
        enriched_df = self.enrich_logs(parsed_df)
        
        # Scrittura su Delta Lake con partitioning
        query = enriched_df \
            .writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("checkpointLocation", checkpoint_path) \
            .partitionBy("date", "hour") \
            .option("path", output_path) \
            .trigger(processingTime='10 seconds') \
            .start()
        
        print("✅ Streaming avviato! Premi Ctrl+C per fermare...")
        
        try:
            query.awaitTermination()
        except KeyboardInterrupt:
            print("\n🛑 Fermando lo streaming...")
            query.stop()
            print("✅ Streaming fermato")

    def run_batch_analytics(self, delta_path="/tmp/delta-lake/logs"):
        """Esegue analytics batch sui dati Delta Lake"""
        print(f"📈 Eseguendo analytics su: {delta_path}")
        
        # Legge dalla tabella Delta
        logs_df = self.spark.read.format("delta").load(delta_path)
        
        # Registra come vista temporanea per SQL
        logs_df.createOrReplaceTempView("logs")
        
        # Analytics queries
        print("\n=== TOP 10 ENDPOINT PIÙ RICHIESTI ===")
        top_endpoints = self.spark.sql("""
            SELECT endpoint, COUNT(*) as request_count
            FROM logs 
            WHERE date >= current_date() - 1
            GROUP BY endpoint
            ORDER BY request_count DESC
            LIMIT 10
        """)
        top_endpoints.show()
        
        print("\n=== ERRORI PER ORA ===")
        errors_by_hour = self.spark.sql("""
            SELECT date, hour, 
                   COUNT(*) as total_requests,
                   SUM(CASE WHEN is_error THEN 1 ELSE 0 END) as error_count,
                   ROUND(AVG(response_time), 2) as avg_response_time
            FROM logs 
            WHERE date >= current_date() - 1
            GROUP BY date, hour
            ORDER BY date DESC, hour DESC
        """)
        errors_by_hour.show()
        
        print("\n=== STATISTICHE UTENTI ===")
        user_stats = self.spark.sql("""
            SELECT 
                COUNT(DISTINCT user_id) as unique_users,
                COUNT(DISTINCT session_id) as unique_sessions,
                COUNT(DISTINCT ip) as unique_ips
            FROM logs 
            WHERE date >= current_date() - 1 AND user_id IS NOT NULL
        """)
        user_stats.show()

    def optimize_delta_table(self, delta_path="/tmp/delta-lake/logs"):
        """Ottimizza la tabella Delta Lake"""
        print(f"🔧 Ottimizzando tabella Delta: {delta_path}")
        
        # Registra la tabella Delta
        self.spark.sql(f"CREATE TABLE IF NOT EXISTS logs USING DELTA LOCATION '{delta_path}'")
        
        # Ottimizzazione e compattazione
        self.spark.sql("OPTIMIZE logs ZORDER BY (endpoint, status_code)")
        
        # Vacuum per rimuovere file vecchi (mantieni 7 giorni)
        self.spark.sql("VACUUM logs RETAIN 168 HOURS")
        
        print("✅ Ottimizzazione completata")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Spark Structured Streaming processor for logs')
    parser.add_argument('--mode', choices=['stream', 'analytics', 'optimize'], 
                       default='stream', help='Modalità di esecuzione')
    parser.add_argument('--kafka-servers', default='localhost:9092', help='Kafka bootstrap servers')
    parser.add_argument('--topic', default='web-logs', help='Kafka topic')
    parser.add_argument('--output-path', default='/tmp/delta-lake/logs', help='Delta Lake output path')
    parser.add_argument('--checkpoint-path', default='/tmp/checkpoints/logs', help='Checkpoint location')
    
    args = parser.parse_args()
    
    processor = LogStreamProcessor()
    
    if args.mode == 'stream':
        processor.start_streaming(
            kafka_servers=args.kafka_servers,
            topic=args.topic,
            output_path=args.output_path,
            checkpoint_path=args.checkpoint_path
        )
    elif args.mode == 'analytics':
        processor.run_batch_analytics(args.output_path)
    elif args.mode == 'optimize':
        processor.optimize_delta_table(args.output_path)