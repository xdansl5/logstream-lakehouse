# Iceberg Analytics Server with DuckDB

A Node.js service for Apache Iceberg analytics backend using DuckDB as the query engine.

## Overview

This service provides a robust analytics backend that:
- Uses DuckDB for fast in-memory analytics
- Creates and manages Iceberg-compatible tables
- Generates sample log data for testing
- Exports data to Parquet format for Iceberg compatibility
- Provides comprehensive logging and error handling

## Features

- **DuckDB Integration**: Uses the native `duckdb` npm package for optimal performance
- **Async/Await Support**: Full Promise-based API for modern Node.js development
- **Comprehensive Logging**: Winston-based logging with structured output
- **Sample Data Generation**: Creates realistic log data for testing and development
- **Parquet Export**: Exports data in Iceberg-compatible Parquet format
- **Table Management**: Full CRUD operations on analytics tables
- **Error Handling**: Robust error handling with detailed logging

## Prerequisites

- Node.js 18+ (tested on Node 22.16.0)
- WSL2/Linux environment
- npm or yarn package manager

## Installation

1. Navigate to the server directory:
```bash
cd server
```

2. Install dependencies:
```bash
npm install
```

## Dependencies

The service uses the following key dependencies:

- **duckdb**: Native DuckDB bindings for Node.js
- **winston**: Structured logging framework
- **apache-arrow**: Arrow data format support
- **parquet-wasm**: Parquet file format support

## Usage

### Basic Service Initialization

```javascript
import IcebergService from './icebergService.js';

const service = new IcebergService();

try {
  await service.initialize();
  console.log('Service initialized successfully');
} catch (error) {
  console.error('Failed to initialize service:', error);
}
```

### Creating Sample Data

```javascript
// Sample data is automatically created during initialization
// You can also manually trigger it:
await service.createSampleLogsTable();
```

### Executing Queries

```javascript
// Execute custom SQL queries
const result = await service.executeQuery('SELECT COUNT(*) as count FROM logs');
console.log(`Total records: ${result.data[0].count}`);

// Get table statistics
const stats = await service.getTableStats('logs');
console.log(`Average response time: ${stats.avg_response_time}ms`);
```

### Table Operations

```javascript
// Get table schema
const schema = await service.getTableSchema('logs');

// Get sample data
const data = await service.getTableData('logs', 10);

// Check if table exists
const exists = await service.tableExists('logs');
```

### Data Export

```javascript
// Export table to Parquet format
await service.exportToParquet('logs');
// File will be saved to: data/iceberg/logs.parquet
```

## Table Schema

### Logs Table

The service creates a comprehensive logs table with the following structure:

| Column | Type | Description |
|--------|------|-------------|
| id | VARCHAR | Unique log identifier |
| timestamp | TIMESTAMP | Log timestamp |
| level | VARCHAR | Log level (INFO, WARN, ERROR, DEBUG) |
| source | VARCHAR | Source system identifier |
| message | VARCHAR | Log message content |
| ip | VARCHAR | IP address |
| status | INTEGER | HTTP status code |
| response_time | INTEGER | Response time in milliseconds |
| endpoint | VARCHAR | API endpoint |
| user_id | VARCHAR | User identifier |
| session_id | VARCHAR | Session identifier |

### Iceberg Metadata Table

| Column | Type | Description |
|--------|------|-------------|
| table_name | VARCHAR | Name of the table |
| schema_version | INTEGER | Schema version number |
| last_updated | TIMESTAMP | Last update timestamp |
| record_count | INTEGER | Total number of records |
| metadata | VARCHAR | JSON metadata string |

## Configuration

The service can be configured using environment variables:

- `ICEBERG_TABLE_PATH`: Path for storing Iceberg data files (default: `../data/iceberg`)

## Logging

The service uses Winston for comprehensive logging:

- **Console Output**: Colorized, human-readable logs
- **File Output**: Structured JSON logs saved to `logs/iceberg-service.log`
- **Log Levels**: info, warn, error with full stack traces

## Performance

- **In-Memory Database**: DuckDB runs entirely in memory for maximum performance
- **Batch Processing**: Sample data insertion uses batch operations for efficiency
- **Optimized Queries**: Uses DuckDB's optimized SQL engine for fast analytics

## Error Handling

The service includes comprehensive error handling:

- **Initialization Errors**: Detailed logging of startup issues
- **Query Errors**: Graceful handling of SQL execution failures
- **Resource Cleanup**: Proper cleanup on service shutdown
- **Fallback Mechanisms**: Continues operation even if some operations fail

## Development

### Running the Service

```bash
# Development mode with auto-restart
npm run dev

# Production mode
npm start
```

### Testing

The service includes comprehensive error handling and logging to help with debugging and development.

## Troubleshooting

### Common Issues

1. **DuckDB Installation**: If you encounter native compilation issues, ensure you have the necessary build tools:
   ```bash
   sudo apt-get update
   sudo apt-get install build-essential python3
   ```

2. **Memory Issues**: The service uses in-memory DuckDB. For large datasets, consider using file-based storage.

3. **Permission Issues**: Ensure the service has write permissions to the data and logs directories.

### Performance Tuning

- **Batch Size**: Adjust the `batchSize` in `createSampleLogsTable()` for optimal memory usage
- **Log Level**: Set Winston log level to 'warn' or 'error' in production for better performance

## License

This project is part of the Iceberg Analytics Backend system.

## Support

For issues and questions, check the logs in the `logs/` directory and review the error messages in the console output.