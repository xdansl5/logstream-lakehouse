# 🧪 Iceberg Analytics Dashboard

Un dashboard interattivo per l'analisi di log in tempo reale utilizzando Apache Iceberg e DuckDB.

## ✨ Caratteristiche

- **Database Persistente**: DuckDB con storage persistente per i dati dei log
- **Query SQL Interattive**: Interfaccia web per eseguire query SQL sui log
- **Generazione Realistica**: Log simulati che riflettono scenari reali di produzione
- **Analisi in Tempo Reale**: Metriche e grafici aggiornati dinamicamente
- **API RESTful**: Backend completo con endpoint per query e analytics

## 🚀 Avvio Rapido

### Prerequisiti

- Node.js 18+ 
- npm o yarn

### Installazione e Avvio

1. **Clona il repository**
   ```bash
   git clone <repository-url>
   cd iceberg-analytics-dashboard
   ```

2. **Avvia il progetto con un comando**
   ```bash
   ./start-project.sh
   ```

   Questo script:
   - Installa le dipendenze del backend e frontend
   - Avvia il server backend su porta 3001
   - Avvia il frontend su porta 5173
   - Genera automaticamente dati di log realistici

3. **Apri il browser**
   - Frontend: http://localhost:5173
   - Backend API: http://localhost:3001
   - Health Check: http://localhost:3001/health

## 🏗️ Architettura

### Backend (Porta 3001)
- **Express.js**: Server web con API RESTful
- **DuckDB**: Database SQL embedded con storage persistente
- **Winston**: Logging strutturato
- **CORS**: Supporto per richieste cross-origin

### Frontend (Porta 5173)
- **React + TypeScript**: Interfaccia utente moderna
- **Tailwind CSS**: Styling responsive
- **React Query**: Gestione stato e cache
- **Shadcn/ui**: Componenti UI riutilizzabili

### Database Schema

La tabella `logs` contiene:
- `id`: Identificatore univoco del log
- `timestamp`: Timestamp dell'evento
- `level`: Livello del log (INFO, WARN, ERROR, DEBUG)
- `source`: Fonte del log (spark-streaming, kafka-consumer, etc.)
- `message`: Messaggio descrittivo
- `ip`: Indirizzo IP della richiesta
- `status`: Codice di stato HTTP
- `response_time`: Tempo di risposta in millisecondi
- `endpoint`: Endpoint API chiamato
- `user_id`: ID utente (se autenticato)
- `session_id`: ID sessione
- `method`: Metodo HTTP (GET, POST, PUT, DELETE)
- `user_agent`: User agent del browser
- `bytes_sent`: Byte inviati nella risposta
- `referer`: URL di riferimento

## 📊 Query SQL di Esempio

### Analisi degli Errori
```sql
SELECT endpoint, count(*) as error_count,
       avg(response_time) as avg_response_time
FROM logs 
WHERE status >= 400 AND timestamp >= datetime('now', '-1 hour')
GROUP BY endpoint
ORDER BY error_count DESC
LIMIT 5
```

### Performance per Sorgente
```sql
SELECT 
  source,
  count(*) as total_requests,
  avg(response_time) as avg_response_time,
  sum(case when status >= 400 then 1 else 0 end) as errors
FROM logs
WHERE timestamp >= datetime('now', '-6 hours')
GROUP BY source
ORDER BY avg_response_time DESC
```

### Anomalie in Tempo Reale
```sql
SELECT 
  endpoint, source, level,
  count(*) as anomaly_count,
  max(response_time) as max_response_time
FROM logs 
WHERE (level = 'ERROR' OR response_time > 1000)
  AND timestamp >= datetime('now', '-30 minutes')
GROUP BY endpoint, source, level
ORDER BY anomaly_count DESC
```

## 🔧 Configurazione

### Variabili d'Ambiente

Crea un file `.env.local` nella root del progetto:

```env
# Backend Configuration
VITE_API_BASE_URL=http://localhost:3001
VITE_SERVER_URL=http://localhost:3001
VITE_SSE_URL=http://localhost:3001/events

# Frontend Configuration
VITE_APP_TITLE=Iceberg Analytics Dashboard
```

### Personalizzazione

- **Porte**: Modifica le porte nel file `start-project.sh`
- **Database**: Il database DuckDB è salvato in `server/data/analytics.db`
- **Log**: I log del server sono in `server/logs/`

## 📈 Metriche Disponibili

- **Eventi per Secondo**: Tasso di log generati
- **Tasso di Errore**: Percentuale di errori HTTP
- **Tempo di Risposta Medio**: Performance delle API
- **Sessioni Attive**: Numero di sessioni utente
- **Dati Processati**: Volume di dati analizzati
- **Tabelle Delta**: Numero di tabelle Iceberg

## 🐛 Troubleshooting

### Backend non si avvia
1. Verifica che la porta 3001 sia libera
2. Controlla i log in `server/logs/`
3. Verifica le dipendenze con `cd server && npm install`

### Frontend non si connette al backend
1. Verifica che il backend sia in esecuzione su porta 3001
2. Controlla la configurazione CORS nel backend
3. Verifica le variabili d'ambiente nel frontend

### Query SQL falliscono
1. Verifica la sintassi SQL (usa la sintassi DuckDB)
2. Controlla che la tabella `logs` esista
3. Verifica i log del backend per errori specifici

## 🔄 Aggiornamenti

Per aggiornare il sistema:

```bash
git pull origin main
./start-project.sh
```

## 📝 Contributi

1. Fork del repository
2. Crea un branch per la feature (`git checkout -b feature/nuova-feature`)
3. Commit delle modifiche (`git commit -am 'Aggiunge nuova feature'`)
4. Push del branch (`git push origin feature/nuova-feature`)
5. Crea una Pull Request

## 📄 Licenza

Questo progetto è rilasciato sotto licenza MIT. Vedi il file `LICENSE` per i dettagli.

## 🆘 Supporto

Per problemi o domande:
- Apri una issue su GitHub
- Controlla i log del sistema
- Verifica la documentazione dell'API su `/health`

---

**Nota**: Questo è un sistema di demo per scopi educativi. Non utilizzare in produzione senza adeguate modifiche di sicurezza.
