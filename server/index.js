#!/usr/bin/env node
'use strict';

import express from 'express';
import cors from 'cors';
import { Kafka } from 'kafkajs';
import IcebergService from './icebergService.js';
import winston from 'winston';
import { existsSync, mkdirSync } from 'fs';
import { join } from 'path';
import { fileURLToPath } from 'url';
import { dirname } from 'path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// Ensure logs directory exists
const logsDir = join(__dirname, 'logs');
if (!existsSync(logsDir)) {
  mkdirSync(logsDir, { recursive: true });
}

// Configure modern logging
const logger = winston.createLogger({
  level: 'info',
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.errors({ stack: true }),
    winston.format.json()
  ),
  transports: [
    new winston.transports.Console({
      format: winston.format.combine(
        winston.format.colorize(),
        winston.format.simple()
      )
    }),
    new winston.transports.File({ filename: join(logsDir, 'server.log') })
  ]
});

const app = express();
const port = process.env.PORT || 3001;

// Initialize Iceberg service
let icebergService = null;

// Initialize the service
async function initializeIcebergService() {
  try {
    logger.info('Starting Iceberg service initialization...');
    icebergService = new IcebergService();
    await icebergService.initialize();
    logger.info('Iceberg service initialized successfully');
    
    // Keep the service alive by setting up a periodic health check
    setInterval(async () => {
      try {
        if (icebergService && icebergService.initialized) {
          // Perform a simple query to keep the connection alive
          await icebergService.executeQuery('SELECT 1 as health_check');
          logger.debug('Iceberg service health check passed');
        }
      } catch (error) {
        logger.warn('Iceberg service health check failed:', error.message);
        // Don't crash the server, just log the warning
      }
    }, 30000); // Check every 30 seconds
    
    return true;
  } catch (err) {
    logger.error('Failed to initialize Iceberg service:', err);
    // Don't crash the server, just log the error and continue without Iceberg
    icebergService = null;
    return false;
  }
}

// Initialize service on startup - don't wait for it
initializeIcebergService().catch(err => {
  logger.error('Iceberg service initialization failed completely:', err);
  icebergService = null;
});

// Middleware
app.use(cors());
app.use(express.json());

// Health check endpoint
app.get('/health', (req, res) => {
  res.json({ 
    status: 'healthy', 
    service: 'iceberg-analytics-server',
    timestamp: new Date().toISOString(),
    version: '2.0.0',
    icebergService: icebergService ? 'initialized' : 'not available'
  });
});

// Query execution endpoint
app.post('/api/query', async (req, res) => {
  try {
    if (!icebergService) {
      return res.status(503).json({ 
        error: 'Iceberg service not available',
        message: 'Service is still initializing or failed to initialize'
      });
    }

    const { query } = req.body;
    
    if (!query) {
      return res.status(400).json({ error: 'Query is required' });
    }

    logger.info(`Executing query: ${query}`);
    const result = await icebergService.executeQuery(query);
    
    res.json({
      success: true,
      data: result.data,
      executionTime: result.executionTime,
      rowCount: result.rowCount,
      query: result.query
    });
  } catch (error) {
    logger.error('Query execution error:', error);
    res.status(500).json({ 
      error: 'Query execution failed', 
      details: error.message 
    });
  }
});

// Get table schema
app.get('/api/tables/:name/schema', async (req, res) => {
  try {
    if (!icebergService) {
      return res.status(503).json({ 
        error: 'Iceberg service not available',
        message: 'Service is still initializing or failed to initialize'
      });
    }

    const { name } = req.params;
    const schema = await icebergService.getTableSchema(name);
    
    res.json({
      success: true,
      tableName: name,
      schema: schema
    });
  } catch (error) {
    logger.error('Schema retrieval error:', error);
    res.status(500).json({ 
      error: 'Failed to retrieve schema', 
      details: error.message 
    });
  }
});

// Get table data
app.get('/api/tables/:name/data', async (req, res) => {
  try {
    if (!icebergService) {
      return res.status(503).json({ 
        error: 'Iceberg service not available',
        message: 'Service is still initializing or failed to initialize'
      });
    }

    const { name } = req.params;
    const { limit = 100 } = req.query;
    const data = await icebergService.getTableData(name, parseInt(limit));
    
    res.json({
      success: true,
      tableName: name,
      data: data,
      limit: parseInt(limit)
    });
  } catch (error) {
    logger.error('Data retrieval error:', error);
    res.status(500).json({ 
      error: 'Failed to retrieve data', 
      details: error.message 
    });
  }
});

// Get table statistics
app.get('/api/tables/:name/stats', async (req, res) => {
  try {
    if (!icebergService) {
      return res.status(503).json({ 
        error: 'Iceberg service not available',
        message: 'Service is still initializing or failed to initialize'
      });
    }

    const { name } = req.params;
    const stats = await icebergService.getTableStats(name);
    
    res.json({
      success: true,
      tableName: name,
      stats: stats
    });
  } catch (error) {
    logger.error('Stats retrieval error:', error);
    res.status(500).json({ 
      error: 'Failed to retrieve statistics', 
      details: error.message 
    });
  }
});

// List available tables
app.get('/api/tables', async (req, res) => {
  try {
    // For now, return the logs table
    // In a real implementation, you'd scan the Iceberg catalog
    res.json({
      success: true,
      tables: [
        {
          name: 'logs',
          type: 'iceberg',
          recordCount: icebergService ? 10000 : 'unknown',
          lastUpdated: new Date().toISOString(),
          status: icebergService ? 'available' : 'initializing'
        }
      ]
    });
  } catch (error) {
    logger.error('Table listing error:', error);
    res.status(500).json({ 
      error: 'Failed to list tables', 
      details: error.message 
    });
  }
});

// Kafka integration endpoint
app.post('/api/ingest', async (req, res) => {
  try {
    const { data } = req.body;
    
    if (!data || !Array.isArray(data)) {
      return res.status(400).json({ error: 'Data array is required' });
    }

    logger.info(`Ingesting ${data.length} records`);
    
    // In a real implementation, you'd write to Iceberg
    // For now, we'll just log the ingestion
    res.json({
      success: true,
      message: `Ingested ${data.length} records`,
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    logger.error('Data ingestion error:', error);
    res.status(500).json({ 
      error: 'Data ingestion failed', 
      details: error.message 
    });
  }
});

// Server-Sent Events for real-time updates
app.get('/events', (req, res) => {
  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive'
  });

  // Send initial connection message
  res.write(`data: ${JSON.stringify({
    type: 'connection',
    message: 'Connected to Iceberg Analytics Server',
    timestamp: new Date().toISOString()
  })}\n\n`);

  // Send periodic updates
  const interval = setInterval(() => {
    res.write(`data: ${JSON.stringify({
      type: 'heartbeat',
      timestamp: new Date().toISOString(),
      activeConnections: 1
    })}\n\n`);
  }, 30000);

  req.on('close', () => {
    clearInterval(interval);
    logger.info('SSE connection closed');
  });
});

// Error handling middleware
app.use((error, req, res, next) => {
  logger.error('Unhandled error:', error);
  res.status(500).json({ 
    error: 'Internal server error',
    message: error.message 
  });
});

// Graceful shutdown
process.on('SIGTERM', async () => {
  logger.info('SIGTERM received, shutting down gracefully');
  if (icebergService) {
    await icebergService.close();
  }
  process.exit(0);
});

process.on('SIGINT', async () => {
  logger.info('SIGINT received, shutting down gracefully');
  if (icebergService) {
    await icebergService.close();
  }
  process.exit(0);
});

// Start server
app.listen(port, () => {
  logger.info(`🚀 Iceberg Analytics Server running on port ${port}`);
  logger.info(`📊 Service: Apache Iceberg + DuckDB`);
  logger.info(`🔍 Health check: http://localhost:${port}/health`);
  logger.info(`📝 API docs: http://localhost:${port}/api/tables`);
  logger.info(`🌐 CORS enabled for cross-origin requests`);
});

export default app;