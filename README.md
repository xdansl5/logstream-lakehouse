# Iceberg Analytics Platform

Una piattaforma completa per l'analisi dei dati con Apache Iceberg, DuckDB e React.

## 🚀 Avvio Rapido

### Opzione 1: Avvio Automatico (Raccomandato)
```bash
# Avvia sia il backend che il frontend con un comando
npm run start-project
```

### Opzione 2: Avvio Manuale
```bash
# Terminal 1: Avvia il backend
cd server && npm start

# Terminal 2: Avvia il frontend
npm run dev
```

## 📋 Prerequisiti

- **Node.js 18+** (testato su Node 22.16.0)
- **npm** o **yarn**
- **WSL2/Linux** (per compatibilità DuckDB)

## 🏗️ Struttura del Progetto

```
iceberg-analytics/
├── server/                 # Backend Node.js + DuckDB
│   ├── index.js           # Server Express principale
│   ├── icebergService.js  # Servizio Iceberg con DuckDB
│   ├── start.js           # Script di avvio robusto
│   └── package.json       # Dipendenze backend
├── src/                    # Frontend React + TypeScript
│   ├── components/         # Componenti UI
│   ├── services/           # Servizi API
│   └── config/             # Configurazione
├── start-project.sh        # Script di avvio completo
└── package.json            # Dipendenze frontend
```

## 🔧 Installazione

1. **Clona il repository**
```bash
git clone <repository-url>
cd iceberg-analytics
```

2. **Installa le dipendenze**
```bash
# Dipendenze frontend
npm install

# Dipendenze backend
cd server && npm install && cd ..
```

3. **Crea le directory necessarie**
```bash
mkdir -p server/logs server/data/iceberg
```

## 🚀 Avvio dei Servizi

### Backend (Porta 3001)
```bash
cd server
npm start
```

**Endpoint disponibili:**
- `GET /health` - Stato del servizio
- `POST /api/query` - Esecuzione query SQL
- `GET /api/tables` - Lista tabelle disponibili
- `GET /api/tables/:name/schema` - Schema tabella
- `GET /api/tables/:name/data` - Dati tabella
- `GET /api/tables/:name/stats` - Statistiche tabella

### Frontend (Porta 8080)
```bash
npm run dev
```

**URL:**
- Frontend: http://localhost:8080
- Backend: http://localhost:3001
- Health Check: http://localhost:3001/health

## 🐛 Risoluzione Problemi

### Il server si termina subito
1. **Controlla i log**: `server/logs/server.log`
2. **Verifica le dipendenze**: `cd server && npm install`
3. **Controlla la versione Node.js**: `node --version` (deve essere 18+)

### Il frontend non si connette al backend
1. **Verifica che il backend sia in esecuzione**: `curl http://localhost:3001/health`
2. **Controlla la configurazione CORS** nel server
3. **Verifica le variabili d'ambiente** in `.env.local`

### Errori DuckDB
1. **Controlla i permessi**: `chmod +x start-project.sh`
2. **Verifica le directory**: `mkdir -p server/logs server/data/iceberg`
3. **Riavvia il servizio**: `npm run start-project`

## 📊 Funzionalità

### Backend
- ✅ **DuckDB Integration**: Database in-memory ad alte prestazioni
- ✅ **Iceberg Tables**: Creazione e gestione tabelle Iceberg
- ✅ **Sample Data**: Generazione automatica dati di test
- ✅ **Parquet Export**: Esportazione dati in formato Iceberg
- ✅ **REST API**: Endpoint completi per l'analisi
- ✅ **Real-time Events**: Server-Sent Events per aggiornamenti live

### Frontend
- ✅ **React 18**: Interfaccia moderna e reattiva
- ✅ **TypeScript**: Tipizzazione completa
- ✅ **Shadcn/ui**: Componenti UI professionali
- ✅ **Real-time Updates**: Aggiornamenti live dal backend
- ✅ **Responsive Design**: Ottimizzato per tutti i dispositivi

## 🔍 Monitoraggio

### Log del Backend
- **Console**: Output colorato e strutturato
- **File**: `server/logs/server.log` (formato JSON)
- **Livelli**: info, warn, error

### Stato del Servizio
- **Health Check**: http://localhost:3001/health
- **Frontend Status**: Componente BackendStatus nella dashboard
- **Real-time Events**: SSE per monitoraggio continuo

## 🚀 Deployment

### Sviluppo
```bash
npm run start-project
```

### Produzione
```bash
# Build frontend
npm run build

# Avvia backend
cd server && npm start
```

### Docker
```bash
docker-compose up -d
```

## 📚 Documentazione API

### Query SQL
```bash
curl -X POST http://localhost:3001/api/query \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT COUNT(*) as count FROM logs"}'
```

### Statistiche Tabella
```bash
curl http://localhost:3001/api/tables/logs/stats
```

### Dati Tabella
```bash
curl "http://localhost:3001/api/tables/logs/data?limit=10"
```

## 🤝 Contributi

1. Fork il repository
2. Crea un branch per la feature: `git checkout -b feature/nuova-funzionalita`
3. Commit le modifiche: `git commit -am 'Aggiungi nuova funzionalità'`
4. Push al branch: `git push origin feature/nuova-funzionalita`
5. Crea una Pull Request

## 📄 Licenza

Questo progetto è rilasciato sotto licenza MIT.

## 🆘 Supporto

Per problemi e domande:
1. Controlla i log in `server/logs/`
2. Verifica la documentazione API
3. Controlla lo stato del servizio nel frontend
4. Apri una issue su GitHub

---

**🎉 Pronto per l'analisi dei dati!**
