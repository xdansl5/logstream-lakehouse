# Delta Lake Streaming Analytics Platform

A modern web interface for querying Delta Lake databases with real-time streaming data from Spark processing pipelines.

## Features

- **Real-time Delta Lake Querying**: Execute Spark SQL queries directly on your Delta Lake tables
- **Live Data Streaming**: View real-time logs and metrics from Spark streaming processing
- **Interactive Analytics**: Explore table schemas and get query suggestions
- **Performance Monitoring**: Track query execution times and results
- **Modern UI**: Beautiful, responsive interface built with React and Tailwind CSS

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Web Interface │    │   Express API   │    │   Delta Lake    │
│   (React/TS)    │◄──►│   Server        │◄──►│   Database      │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   SSE Stream    │    │   Kafka         │    │   Spark         │
│   (Real-time)   │    │   Consumer      │    │   Streaming     │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## Quick Start

### Prerequisites

- Node.js 18+ 
- npm or yarn
- Delta Lake database (or use the included sample data)

### Installation

1. **Clone and install dependencies:**
   ```bash
   git clone <repository-url>
   cd delta-lake-analytics
   npm install
   ```

2. **Install server dependencies:**
   ```bash
   cd server
   npm install
   cd ..
   ```

3. **Start the development server:**
   ```bash
   # Terminal 1: Start the API server
   npm run server
   
   # Terminal 2: Start the frontend
   npm run dev
   ```

4. **Open your browser:**
   Navigate to `http://localhost:5173` to access the web interface.

## Configuration

### Environment Variables

Create a `.env` file in the root directory:

```env
# Server Configuration
VITE_SERVER_URL=http://localhost:4000
VITE_SSE_URL=http://localhost:4000/events

# Delta Lake Configuration
DELTA_TABLE_PATH=./data/delta_lake

# Kafka Configuration (optional)
KAFKA_BROKERS=localhost:9092
KAFKA_TOPIC=web-logs
KAFKA_GROUP_ID=ui-bridge-group
```

### Delta Lake Setup

The platform includes a sample Delta Lake service that simulates real Delta Lake functionality. In a production environment, you would:

1. **Connect to your actual Delta Lake instance**
2. **Configure Spark SQL connectivity**
3. **Set up proper authentication and security**

## Usage

### 1. Explore Tables

- Navigate to the "Delta Lake Analytics" section
- Click on table names to view schemas
- See available columns and data types

### 2. Execute Queries

- Use the query editor to write Spark SQL queries
- Click "Execute Query" to run against your Delta Lake
- View results in the formatted table below

### 3. Query Suggestions

The platform provides pre-built query templates:

- **Recent Activity**: Latest log entries
- **Error Analysis**: Error patterns and trends
- **Performance Metrics**: Response time analysis
- **User Session Analysis**: User behavior insights
- **Anomaly Detection**: Identify unusual patterns
- **Hourly Trends**: Time-based analytics

### 4. Sample Queries

```sql
-- Error analysis
SELECT 
  endpoint,
  count(*) as error_count,
  avg(response_time) as avg_response_time
FROM delta_lake.logs 
WHERE status >= 400 
  AND timestamp >= current_timestamp() - interval 24 hours
GROUP BY endpoint
ORDER BY error_count DESC;

-- Performance metrics
SELECT 
  source,
  count(*) as total_requests,
  avg(response_time) as avg_response_time
FROM delta_lake.logs
WHERE timestamp >= current_timestamp() - interval 1 day
GROUP BY source
ORDER BY total_requests DESC;

-- User session analysis
SELECT 
  user_id,
  count(distinct session_id) as unique_sessions,
  count(*) as total_requests
FROM delta_lake.logs
WHERE timestamp >= current_date()
GROUP BY user_id
HAVING total_requests > 10
ORDER BY total_requests DESC;
```

## API Endpoints

### Query Execution
```http
POST /api/query
Content-Type: application/json

{
  "query": "SELECT * FROM delta_lake.logs LIMIT 10"
}
```

### Table Schema
```http
GET /api/tables/{tableName}/schema
```

### List Tables
```http
GET /api/tables
```

### Real-time Events
```http
GET /events
```

## Development

### Project Structure

```
├── src/
│   ├── components/          # React components
│   │   ├── SqlQuery.tsx     # Query interface
│   │   ├── DeltaLakeExplorer.tsx # Table explorer
│   │   └── ...
│   ├── contexts/            # React contexts
│   │   └── DataContext.tsx  # Data management
│   └── pages/               # Page components
├── server/
│   ├── index.js             # Express API server
│   └── deltaLakeService.js  # Delta Lake integration
└── data/
    └── delta_lake/          # Sample Delta Lake data
```

### Adding New Features

1. **New Query Types**: Extend the `DeltaLakeService` class
2. **UI Components**: Add React components in `src/components/`
3. **API Endpoints**: Add routes in `server/index.js`

## Production Deployment

### Docker Setup

```dockerfile
# Frontend
FROM node:18-alpine AS frontend
WORKDIR /app
COPY package*.json ./
RUN npm ci --only=production
COPY . .
RUN npm run build

# Backend
FROM node:18-alpine AS backend
WORKDIR /app
COPY server/package*.json ./
RUN npm ci --only=production
COPY server/ ./

EXPOSE 4000
CMD ["node", "index.js"]
```

### Environment Configuration

Set production environment variables:

```env
NODE_ENV=production
DELTA_TABLE_PATH=/data/delta_lake
KAFKA_BROKERS=kafka1:9092,kafka2:9092
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

MIT License - see LICENSE file for details.

## Support

For issues and questions:
- Create an issue in the repository
- Check the documentation
- Review the sample queries and configurations
