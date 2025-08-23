# 🚀 Iceberg Analytics Platform

Una piattaforma moderna di analytics basata su **Apache Iceberg**, **DuckDB** e **Apache Arrow** per l'analisi di dati in tempo reale con un'interfaccia web React moderna.

## ✨ Caratteristiche Principali

- **🔍 Query SQL Avanzate**: Esecuzione di query SQL complesse su dati streaming
- **📊 Analytics in Tempo Reale**: Aggregazioni e metriche live
- **🌐 Interfaccia Web Moderna**: UI React con Tailwind CSS e Shadcn/ui
- **📈 Visualizzazioni Interattive**: Grafici e dashboard dinamici
- **🔄 Streaming Data**: Integrazione Kafka per dati in tempo reale
- **📁 Formati Multipli**: Supporto per Iceberg, Parquet e Arrow
- **🚀 Cross-Platform**: Nessuna compilazione nativa richiesta

## 🏗️ Architettura

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   React Frontend│    │  Node.js Server │    │   Apache Kafka  │
│   (Port 5173)   │◄──►│   (Port 3001)   │◄──►│   (Port 9092)   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                │
                                ▼
                       ┌─────────────────┐
                       │   DuckDB Engine │
                       │  + Apache Arrow │
                       │  + Iceberg      │
                       └─────────────────┘
```

## 🛠️ Stack Tecnologico

### Frontend
- **React 18** + **TypeScript**
- **Vite** per build veloce
- **Tailwind CSS** + **Shadcn/ui**
- **React Query** per state management
- **Recharts** per visualizzazioni

### Backend
- **Node.js 18+** con **ES Modules**
- **Express.js** per API REST
- **DuckDB** per database embedded
- **Apache Arrow** per formati dati
- **Apache Iceberg** per tabelle
- **Winston** per logging moderno

### Data Processing
- **Kafka** per streaming
- **Parquet** per storage efficiente
- **Arrow** per formati ottimizzati

## 🚀 Installazione Rapida

### Prerequisiti
- **Node.js 18+** (LTS raccomandato)
- **npm** o **yarn**
- **Git**

### Setup Automatico
```bash
# 1. Clona il repository
git clone <repository-url>
cd iceberg-analytics-platform

# 2. Esegui lo script di setup
chmod +x scripts/setup.sh
./scripts/setup.sh

# 3. Avvia il server
npm run server

# 4. In un nuovo terminale, avvia il frontend
npm run dev

# 5. Apri http://localhost:5173
```

### Setup Manuale
```bash
# 1. Installa le dipendenze frontend
npm install

# 2. Installa le dipendenze server
cd server
npm install
cd ..

# 3. Crea le directory necessarie
mkdir -p data/iceberg data/arrow logs

# 4. Avvia i servizi
npm run server    # Terminal 1
npm run dev       # Terminal 2
```

## 🧪 Testing

```bash
# Test completo del server
npm run test:iceberg

# Test specifici
curl http://localhost:3001/health
curl http://localhost:3001/api/tables
```

## 📊 API Endpoints

### Core API
- `GET /health` - Health check del server
- `GET /api/tables` - Lista delle tabelle disponibili
- `GET /api/tables/:name/schema` - Schema di una tabella
- `GET /api/tables/:name/data` - Dati di una tabella
- `GET /api/tables/:name/stats` - Statistiche di una tabella

### Query Execution
- `POST /api/query` - Esegue query SQL
- `POST /api/ingest` - Ingestione di nuovi dati

### Real-time
- `GET /events` - Server-Sent Events per aggiornamenti live

## 🐳 Docker Deployment

```bash
# Avvia tutti i servizi
docker-compose up -d

# Verifica lo stato
docker-compose ps

# Logs
docker-compose logs -f backend

# Ferma i servizi
docker-compose down
```

## 🔧 Configurazione

### Variabili d'Ambiente
```bash
# Server
PORT=3001
NODE_ENV=production

# Iceberg
ICEBERG_TABLE_PATH=./data/iceberg
ARROW_DATA_PATH=./data/arrow

# Kafka (opzionale)
KAFKA_BROKERS=localhost:9092
KAFKA_TOPIC=web-logs
KAFKA_GROUP_ID=ui-bridge-group
```

### File di Configurazione
- `.env` - Variabili d'ambiente
- `server/package.json` - Dipendenze server
- `package.json` - Dipendenze frontend
- `docker-compose.yml` - Orchestrazione Docker

## 📈 Esempi di Query

### Analisi degli Errori
```sql
SELECT endpoint, COUNT(*) as error_count, AVG(response_time) as avg_response_time
FROM logs 
WHERE status >= 400 
  AND timestamp >= current_timestamp() - INTERVAL 24 HOURS
GROUP BY endpoint
ORDER BY error_count DESC
```

### Metriche di Performance
```sql
SELECT source, COUNT(*) as total_requests, AVG(response_time) as avg_response_time
FROM logs
WHERE timestamp >= current_timestamp() - INTERVAL 1 DAY
GROUP BY source
ORDER BY total_requests DESC
```

### Analisi delle Sessioni
```sql
SELECT user_id, COUNT(DISTINCT session_id) as unique_sessions, COUNT(*) as total_requests
FROM logs
WHERE timestamp >= current_date()
GROUP BY user_id
HAVING total_requests > 10
ORDER BY total_requests DESC
```

## 🔍 Troubleshooting

### Problemi Comuni

#### Server non si avvia
```bash
# Verifica la porta
lsof -i :3001

# Controlla i log
tail -f logs/server.log
```

#### Dipendenze non installate
```bash
# Rimuovi node_modules e reinstalla
rm -rf node_modules package-lock.json
npm install
```

#### Problemi di permessi
```bash
# Verifica i permessi delle directory
ls -la data/ logs/
chmod -R 755 data/ logs/
```

### Log e Debug
- **Server logs**: `logs/server.log`
- **Iceberg logs**: `logs/iceberg-service.log`
- **Arrow logs**: `logs/arrow-service.log`

## 🚀 Sviluppo

### Struttura del Progetto
```
├── src/                    # Frontend React
├── server/                 # Backend Node.js
│   ├── index.js           # Server principale
│   ├── icebergService.js  # Servizio Iceberg
│   └── arrowService.js    # Servizio Arrow
├── scripts/                # Script di setup e utility
├── data/                   # Dati e tabelle
│   ├── iceberg/           # Tabelle Iceberg
│   └── arrow/             # File Arrow/Parquet
└── logs/                   # File di log
```

### Comandi di Sviluppo
```bash
# Sviluppo frontend
npm run dev

# Sviluppo backend con hot reload
cd server && npm run dev

# Build di produzione
npm run build

# Linting
npm run lint

# Testing
npm run test:iceberg
```

## 📚 Risorse Aggiuntive

- [Apache Iceberg Documentation](https://iceberg.apache.org/)
- [DuckDB Documentation](https://duckdb.org/docs/)
- [Apache Arrow Documentation](https://arrow.apache.org/)
- [React Documentation](https://react.dev/)
- [Tailwind CSS](https://tailwindcss.com/)

## 🤝 Contributi

1. Fork del repository
2. Crea un branch per la feature (`git checkout -b feature/AmazingFeature`)
3. Commit delle modifiche (`git commit -m 'Add some AmazingFeature'`)
4. Push al branch (`git push origin feature/AmazingFeature`)
5. Apri una Pull Request

## 📄 Licenza

Questo progetto è rilasciato sotto licenza MIT. Vedi `LICENSE` per i dettagli.

## 🆘 Supporto

- **Issues**: [GitHub Issues](https://github.com/your-repo/issues)
- **Documentazione**: [Wiki](https://github.com/your-repo/wiki)
- **Email**: support@your-domain.com

---

**⭐ Se questo progetto ti è utile, considera di dargli una stella su GitHub!**
