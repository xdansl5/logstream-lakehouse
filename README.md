# Real-Time Data Pipeline Platform

Una piattaforma web interattiva per monitorare e analizzare i dati in tempo reale dalla pipeline Kafka → Spark Streaming → Delta Lake.

## 🚀 Caratteristiche Principali

### Aggiornamento in Tempo Reale
- **Live Log Stream**: Visualizzazione in tempo reale dei log generati dalla pipeline
- **Metriche Dinamiche**: Aggiornamento automatico di tutte le metriche basato sui dati reali
- **Grafici Interattivi**: Chart che si aggiornano automaticamente con i nuovi dati
- **Connessione SSE**: Server-Sent Events per aggiornamenti istantanei

### Dati Reali dalla Pipeline
- **Kafka Integration**: Connessione diretta ai topic Kafka per i log in tempo reale
- **Spark SQL Queries**: Esecuzione di query reali sui dati Delta Lake
- **Delta Lake Analytics**: Interrogazione diretta delle tabelle Delta Lake
- **Nessun Dato Simulato**: Tutti i dati provengono dalla pipeline reale

### Interfaccia Interattiva
- **Status di Connessione**: Indicatori visivi dello stato della pipeline
- **Query Builder**: Interfaccia per eseguire query Spark SQL personalizzate
- **Filtri Avanzati**: Filtraggio dei log per livello, sorgente, endpoint
- **Anomaly Detection**: Rilevamento automatico di anomalie nei dati

## 🏗️ Architettura

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Web Logs      │───▶│     Kafka       │───▶│  Spark Streaming│
│   (Sources)     │    │   (Broker)      │    │   (Processing)  │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                                       │
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Web Platform  │◀───│   API Server    │◀───│   Delta Lake    │
│   (React App)   │    │   (Express)     │    │   (Storage)     │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## 🛠️ Tecnologie Utilizzate

### Frontend
- **React 18** con TypeScript
- **Vite** per build e development
- **shadcn/ui** per componenti UI
- **Tailwind CSS** per styling
- **Recharts** per grafici interattivi
- **React Query** per state management

### Backend
- **Node.js** con Express
- **KafkaJS** per connessione Kafka
- **Server-Sent Events** per streaming real-time
- **REST API** per query e metriche

### Data Pipeline
- **Apache Kafka** per message streaming
- **Apache Spark** per processing
- **Delta Lake** per storage
- **Spark SQL** per querying

## 📦 Installazione e Setup

### Prerequisiti
- Node.js 18+ e npm
- Apache Kafka (running on localhost:9092)
- Apache Spark (running on localhost:7077)
- Delta Lake tables configured

### 1. Clona il Repository
```bash
git clone <YOUR_REPO_URL>
cd data-pipeline-platform
```

### 2. Installa le Dipendenze
```bash
npm install
```

### 3. Configura le Variabili d'Ambiente
```bash
cp .env.example .env
```

Modifica il file `.env` con le tue configurazioni:
```env
# API Configuration
VITE_API_URL=http://localhost:4000

# Kafka Configuration
KAFKA_BROKERS=localhost:9092
KAFKA_TOPIC=web-logs
KAFKA_GROUP_ID=ui-bridge-group

# Spark Configuration
SPARK_MASTER=spark://localhost:7077
DELTA_LAKE_PATH=/tmp/delta-lake

# Server Configuration
PORT=4000
```

### 4. Avvia il Server Backend
```bash
npm run server
```

### 5. Avvia l'Applicazione Frontend
```bash
npm run dev
```

## 🔧 Configurazione della Pipeline

### 1. Setup Kafka
Assicurati che Kafka sia in esecuzione e che il topic `web-logs` esista:
```bash
# Crea il topic se non esiste
kafka-topics.sh --create --topic web-logs --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

### 2. Setup Spark
Avvia Spark con supporto Delta Lake:
```bash
spark-submit --packages io.delta:delta-core_2.12:2.4.0 \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  your-spark-job.py
```

### 3. Generazione Log di Test
Per testare la piattaforma, puoi generare log di test che verranno processati dalla pipeline:

```python
# Esempio di script Python per generare log
import json
import time
import random
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

endpoints = ['/api/users', '/api/orders', '/api/products', '/health', '/metrics']
methods = ['GET', 'POST', 'PUT', 'DELETE']

while True:
    log_entry = {
        'timestamp': time.time() * 1000,
        'method': random.choice(methods),
        'endpoint': random.choice(endpoints),
        'status_code': random.choice([200, 200, 200, 404, 500]),
        'response_time': random.randint(50, 2000),
        'ip': f'192.168.{random.randint(1,10)}.{random.randint(1,255)}',
        'user_id': f'user_{random.randint(1,1000)}',
        'session_id': f'sess_{random.randint(1000,9999)}'
    }
    
    producer.send('web-logs', log_entry)
    time.sleep(random.uniform(0.1, 2.0))
```

## 🎯 Utilizzo della Piattaforma

### 1. Dashboard Principale
- **Metrics Grid**: Visualizza metriche in tempo reale (events/sec, error rate, response time, etc.)
- **Analytics Charts**: Grafici interattivi per volume richieste e performance
- **Live Log Stream**: Stream in tempo reale dei log dalla pipeline

### 2. Query Interface
- **Spark SQL**: Esegui query personalizzate sui dati Delta Lake
- **Sample Queries**: Query predefinite per analisi comuni
- **Real-time Results**: Risultati aggiornati in tempo reale

### 3. Monitoraggio
- **Connection Status**: Indicatori visivi dello stato della pipeline
- **Anomaly Detection**: Rilevamento automatico di errori e anomalie
- **Performance Metrics**: Monitoraggio delle performance in tempo reale

## 🔍 Esempi di Query

### Analisi Errori
```sql
SELECT endpoint, count(*) as error_count,
       avg(response_time) as avg_response_time
FROM delta_lake.logs 
WHERE status >= 400 AND timestamp >= current_timestamp() - interval 1 hour
GROUP BY endpoint
ORDER BY error_count DESC
LIMIT 5
```

### Analisi Sessioni Utente
```sql
SELECT 
  user_id,
  count(distinct session_id) as sessions,
  count(*) as page_views,
  sum(response_time) / count(*) as avg_session_time
FROM delta_lake.logs
WHERE timestamp >= current_date()
GROUP BY user_id
ORDER BY page_views DESC
LIMIT 10
```

### Rilevamento Anomalie
```sql
SELECT 
  endpoint, source, level,
  count(*) as anomaly_count,
  max(response_time) as max_response_time
FROM delta_lake.logs 
WHERE (level = 'ERROR' OR response_time > 1000)
  AND timestamp >= current_timestamp() - interval 30 minutes
GROUP BY endpoint, source, level
ORDER BY anomaly_count DESC
```

## 🚨 Troubleshooting

### Problemi di Connessione
1. **Kafka non raggiungibile**: Verifica che Kafka sia in esecuzione su localhost:9092
2. **Spark non disponibile**: Controlla che Spark sia attivo su localhost:7077
3. **Delta Lake non accessibile**: Verifica i permessi e la configurazione Delta Lake

### Debug
- Controlla i log del server: `npm run server`
- Verifica la connessione SSE: `curl http://localhost:4000/health`
- Testa le API: `curl http://localhost:4000/api/metrics`

## 📈 Metriche e Performance

La piattaforma monitora in tempo reale:
- **Throughput**: Eventi per secondo processati
- **Latency**: Tempo di risposta medio
- **Error Rate**: Percentuale di errori
- **Active Sessions**: Sessioni utente attive
- **Data Processed**: Volume di dati processati

## 🤝 Contributi

1. Fork il repository
2. Crea un branch per la feature (`git checkout -b feature/AmazingFeature`)
3. Commit le modifiche (`git commit -m 'Add some AmazingFeature'`)
4. Push al branch (`git push origin feature/AmazingFeature`)
5. Apri una Pull Request

## 📄 Licenza

Questo progetto è sotto licenza MIT. Vedi il file `LICENSE` per i dettagli.

## 🆘 Supporto

Per supporto e domande:
- Apri una issue su GitHub
- Contatta il team di sviluppo
- Consulta la documentazione della pipeline
