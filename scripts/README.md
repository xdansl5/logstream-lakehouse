# Lakehouse Analytics Platform - Scripts PySpark

Questa directory contiene tutti gli script Python/PySpark per implementare una pipeline completa di analisi dati in tempo reale con architettura Lakehouse.

## 🏗️ Architettura

```
[Log Generator] → [Kafka] → [Spark Streaming] → [Delta Lake] → [Analytics/ML]
                                    ↓
                             [Anomaly Detection]
```

## 📁 File Principali

### `log_generator.py`
Genera log realistici di un web server e li invia a Kafka in tempo reale.

**Caratteristiche:**
- Simula traffico web con endpoint realistici
- Genera errori e anomalie in modo controllato
- Configurabile per rate e durata
- Output in formato JSON

**Uso:**
```bash
# Genera 10 log/secondo per 5 minuti
python3 log_generator.py --rate 10 --duration 300

# Genera continuamente a 5 log/secondo
python3 log_generator.py --rate 5
```

### `streaming_processor.py`
Core della pipeline - elabora i log da Kafka usando Spark Structured Streaming e li scrive su Delta Lake.

**Funzionalità:**
- **Structured Streaming**: Legge da Kafka in tempo reale
- **Data Enrichment**: Arricchisce i log con metadati
- **Delta Lake**: Scrittura transazionale e versionata
- **Partitioning**: Ottimizzazione per query performance
- **Watermarking**: Gestione late-arriving data

**Modalità:**
```bash
# Streaming in tempo reale
python3 streaming_processor.py --mode stream

# Analytics batch sui dati storici
python3 streaming_processor.py --mode analytics

# Ottimizzazione tabelle Delta
python3 streaming_processor.py --mode optimize
```

### `anomaly_detector.py`
Rileva anomalie nei pattern di traffico usando finestre temporali scorrevoli.

**Algoritmi:**
- Traffic spike detection
- Error rate anomalies
- Response time anomalies
- IP-based pattern analysis

**Uso:**
```bash
# Rilevamento real-time
python3 anomaly_detector.py --mode detect

# Analisi anomalie storiche
python3 anomaly_detector.py --mode analyze
```

### `demo_pipeline.py`
Demo orchestrata che avvia tutti i componenti della pipeline.

```bash
# Demo completa (tutti i componenti)
python3 demo_pipeline.py --mode full

# Solo analytics su dati esistenti
python3 demo_pipeline.py --mode analytics
```

## 🚀 Setup e Installazione

### 1. Setup Environment
```bash
chmod +x setup_environment.sh
./setup_environment.sh
```

Questo script:
- Installa Apache Spark
- Configura Delta Lake
- Avvia Kafka con Docker
- Crea le directory necessarie

### 2. Installa Dipendenze Python
```bash
pip install -r requirements.txt
```

### 3. Verifica Kafka
```bash
# Controlla che Kafka sia attivo
docker-compose ps

# Visualizza i topic
docker-compose exec kafka kafka-topics --list --bootstrap-server localhost:29092
```

## 🎯 Concetti Spark Dimostrati

### 1. **Spark Structured Streaming**
```python
# Lettura da Kafka
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "web-logs") \
    .load()
```

### 2. **Delta Lake Integration**
```python
# Scrittura transazionale su Delta Lake
query = enriched_df \
    .writeStream \
    .format("delta") \
    .partitionBy("date", "hour") \
    .option("checkpointLocation", checkpoint_path) \
    .trigger(processingTime='10 seconds') \
    .start()
```

### 3. **Watermarking per Late Data**
```python
windowed_df = logs_stream \
    .withWatermark("timestamp", "2 minutes") \
    .groupBy(window(col("timestamp"), "1 minute")) \
    .count()
```

### 4. **Ottimizzazioni Performance**
```python
# Z-Ordering per query performance
spark.sql("OPTIMIZE logs ZORDER BY (endpoint, status_code)")

# Compaction automatica
spark.sql("VACUUM logs RETAIN 168 HOURS")
```

## 📊 Esempi di Query Analytics

### Top Endpoint per Errori
```sql
SELECT endpoint, COUNT(*) as error_count
FROM logs 
WHERE status_code >= 400 
  AND date >= current_date() - 1
GROUP BY endpoint
ORDER BY error_count DESC
```

### Pattern Temporali
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

### Rilevamento Anomalie
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

## 🔧 Configurazioni Avanzate

### Tuning Spark per Streaming
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

## 🎮 Workflow Tipico

1. **Setup iniziale:**
   ```bash
   ./setup_environment.sh
   ```

2. **Avvia pipeline completa:**
   ```bash
   python3 demo_pipeline.py
   ```

3. **In parallelo, monitora:**
   - Kafka UI: http://localhost:8080
   - Controlla i dati in Delta Lake
   - Esegui query analytics

4. **Analytics interattive:**
   ```bash
   python3 streaming_processor.py --mode analytics
   ```

## 📈 Monitoring e Debugging

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

## 📚 Risorse Aggiuntive

- [Apache Spark Structured Streaming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Delta Lake Documentation](https://docs.delta.io/)
- [Kafka Python Client](https://kafka-python.readthedocs.io/)

---

**Buon divertimento con la tua pipeline Lakehouse! 🚀**