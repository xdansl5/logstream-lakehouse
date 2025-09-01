# Lakehouse Analytics Platform - PySpark Scripts

This directory contains all Python/PySpark scripts to implement a complete real-time data pipeline using a Lakehouse architecture.

## 🏗️ Architecture

```
[Log Generator] → [Kafka] → [Spark Streaming] → [Delta Lake] → [Analytics/ML]
                                    ↓
                             [Anomaly Detection]
```

## 📁 Key Files

### `enhanced_log_generator.py`
Generates realistic web server logs and publishes them to Kafka in real-time.

**Features:**
- Realistic traffic patterns and endpoints
- Controlled error and anomaly generation
- Configurable rate and duration
- JSON output

**Usage:**
```bash
# Generate 10 logs/second for 5 minutes
python3 enhanced_log_generator.py --rate 10 --duration 300

# Generate continuously at 5 logs/second
python3 enhanced_log_generator.py --rate 5
```

### `streaming_processor.py`
Core of the pipeline — processes logs from Kafka with Spark Structured Streaming and writes to Delta Lake.

**Capabilities:**
- **Structured Streaming**: Reads from Kafka in real-time
- **Data Enrichment**: Adds metadata to logs
- **Delta Lake**: Transactional, versioned writes
- **Partitioning**: Optimizes query performance
- **Watermarking**: Manages late-arriving data

**Modes:**
```bash
# Real-time streaming
python3 streaming_processor.py --mode stream

# Batch analytics on historical data
python3 streaming_processor.py --mode analytics

# Delta table optimization
python3 streaming_processor.py --mode optimize
```

### `anomaly_detector.py`
Detects anomalies in traffic patterns using sliding time windows.

**Algorithms:**
- Traffic spike detection
- Error rate anomalies
- Response time anomalies
- IP-based pattern analysis

**Usage:**
```bash
# Real-time detection
python3 anomaly_detector.py --mode detect

# Analyze historical anomalies
python3 anomaly_detector.py --mode analyze
```

### Removed legacy demo scripts
`demo_pipeline.py` and `log_generator.py` were removed in favor of `pipeline_orchestrator.py` and `enhanced_log_generator.py`.

## 🚀 Setup and Installation

### 1. Setup Environment
```bash
chmod +x setup_environment.sh
./setup_environment.sh
```

This script:
- Installs Apache Spark
- Configures Delta Lake
- Starts Kafka via Docker
- Creates required directories

### 2. Install Python Dependencies
```bash
pip install -r requirements.txt
```

### 3. Verify Kafka
```bash
# Check Kafka is running
docker-compose ps

# List topics
docker-compose exec kafka kafka-topics --list --bootstrap-server localhost:29092
```

## 🎯 Spark Concepts Demonstrated

### 1. **Spark Structured Streaming**
```python
# Read from Kafka
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "web-logs") \
    .load()
```

### 2. **Delta Lake Integration**
```python
# Transactional write to Delta Lake
query = enriched_df \
    .writeStream \
    .format("delta") \
    .partitionBy("date", "hour") \
    .option("checkpointLocation", checkpoint_path) \
    .trigger(processingTime='10 seconds') \
    .start()
```

### 3. **Watermarking for Late Data**
```python
windowed_df = logs_stream \
    .withWatermark("timestamp", "2 minutes") \
    .groupBy(window(col("timestamp"), "1 minute")) \
    .count()
```

### 4. **Performance Optimizations**
```python
# Z-Ordering per query performance
spark.sql("OPTIMIZE logs ZORDER BY (endpoint, status_code)")

# Compaction automatica
spark.sql("VACUUM logs RETAIN 168 HOURS")
```

## 📊 Analytics Query Examples

### Top Error Endpoints
```sql
SELECT endpoint, COUNT(*) as error_count
FROM logs 
WHERE status_code >= 400 
  AND date >= current_date() - 1
GROUP BY endpoint
ORDER BY error_count DESC
```

### Temporal Patterns
```sql
SELECT hour, 
       COUNT(*) as requests,
       AVG(response_time) as avg_response_time,
       COUNT(DISTINCT ip) as unique_visitors
FROM logs 
WHERE date = current_date()
GROUP BY hour
ORDER BY hour
```

### Anomaly Detection
```sql
SELECT window.start, endpoint,
       COUNT(*) as request_count,
       AVG(response_time) as avg_response_time
FROM logs_windowed
WHERE request_count > (
    SELECT AVG(request_count) * 3 
    FROM logs_windowed
)
```

## 🔧 Advanced Configuration

### Spark Tuning for Streaming
```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
```

### Delta Lake Time Travel
```python
# Leggi versione specifica
df = spark.read.format("delta").option("versionAsOf", 0).load("/path/to/table")

# Leggi a timestamp specifico
df = spark.read.format("delta").option("timestampAsOf", "2024-01-01").load("/path/to/table")
```

## 🎮 Typical Workflow

1. **Initial setup:**
   ```bash
   ./setup_environment.sh
   ```

2. **Start the full pipeline:**
   ```bash
   python3 demo_pipeline.py
   ```

3. **In parallel, monitor:**
   - Kafka UI: http://localhost:8080
   - Inspect Delta Lake data
   - Run analytics queries

4. **Interactive analytics:**
   ```bash
   python3 streaming_processor.py --mode analytics
   ```

## 📈 Monitoring & Debugging

### Check Streaming Status
```python
# Stato delle query streaming
for stream in spark.streams.active:
    print(f"Stream: {stream.name}, Status: {stream.status}")
```

### Delta Lake History
```sql
DESCRIBE HISTORY logs
```

### Performance Metrics
```python
# Statistiche tabella
spark.sql("DESCRIBE DETAIL logs").show()
```

## 🚨 Troubleshooting

### Kafka Connection Issues
```bash
# Verifica connettività
docker-compose exec kafka kafka-broker-api-versions --bootstrap-server localhost:29092
```

### Delta Lake Permissions
```bash
# Assicurati che le directory abbiano i permessi corretti
chmod -R 755 /tmp/delta-lake/
```

### Memory Issues
```bash
# Aumenta memoria Spark
export SPARK_DRIVER_MEMORY=4g
export SPARK_EXECUTOR_MEMORY=4g
```

## 📚 Additional Resources

- [Apache Spark Structured Streaming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Delta Lake Documentation](https://docs.delta.io/)
- [Kafka Python Client](https://kafka-python.readthedocs.io/)

---

**Enjoy your Lakehouse pipeline! 🚀**