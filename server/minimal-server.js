#!/usr/bin/env node
'use strict';

import express from 'express';
import cors from 'cors';
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

// Configure logging
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
    new winston.transports.File({ filename: join(logsDir, 'minimal-server.log') })
  ]
});

const app = express();
const port = process.env.PORT || 3001;

// Middleware
app.use(cors());
app.use(express.json());

// Health check endpoint
app.get('/health', (req, res) => {
  res.json({ 
    status: 'healthy', 
    service: 'minimal-iceberg-server',
    timestamp: new Date().toISOString(),
    version: '2.0.0',
    message: 'Server running without Iceberg service'
  });
});

// Basic API endpoints
app.get('/api/tables', (req, res) => {
  res.json({
    success: true,
    tables: [
      {
        name: 'logs',
        type: 'iceberg',
        recordCount: 'unknown',
        lastUpdated: new Date().toISOString(),
        status: 'service not available'
      }
    ]
  });
});

app.get('/api/status', (req, res) => {
  res.json({
    success: true,
    message: 'Minimal server is running',
    timestamp: new Date().toISOString(),
    icebergService: 'not initialized'
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

// Start server
const server = app.listen(port, () => {
  logger.info(`🚀 Minimal Iceberg Server running on port ${port}`);
  logger.info(`📊 Service: Basic Express Server (No Iceberg)`);
  logger.info(`🔍 Health check: http://localhost:${port}/health`);
  logger.info(`📝 API docs: http://localhost:${port}/api/tables`);
  logger.info(`🌐 CORS enabled for cross-origin requests`);
});

// Graceful shutdown
process.on('SIGTERM', () => {
  logger.info('SIGTERM received, shutting down gracefully');
  server.close(() => {
    logger.info('Server closed');
    process.exit(0);
  });
});

process.on('SIGINT', () => {
  logger.info('SIGINT received, shutting down gracefully');
  server.close(() => {
    logger.info('Server closed');
    process.exit(0);
  });
});

export default app;