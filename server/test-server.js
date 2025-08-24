#!/usr/bin/env node
'use strict';

import express from 'express';
import cors from 'cors';
import IcebergService from './icebergService.js';

const app = express();
const port = 3002; // Use different port for testing

// Middleware
app.use(cors());
app.use(express.json());

// Initialize Iceberg service
let icebergService = null;

// Initialize the service
async function initializeIcebergService() {
  try {
    console.log('Starting Iceberg service initialization...');
    icebergService = new IcebergService();
    await icebergService.initialize();
    console.log('Iceberg service initialized successfully');
    return true;
  } catch (err) {
    console.error('Failed to initialize Iceberg service:', err);
    icebergService = null;
    return false;
  }
}

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

    console.log(`Executing query: ${query}`);
    const result = await icebergService.executeQuery(query);
    
    res.json({
      success: true,
      data: result.data,
      executionTime: result.executionTime,
      rowCount: result.rowCount,
      query: result.query,
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    console.error('Query execution error:', error);
    res.status(500).json({ 
      error: 'Query execution failed', 
      details: error.message,
      timestamp: new Date().toISOString()
    });
  }
});

// Start server
app.listen(port, async () => {
  console.log(`🚀 Test Server running on port ${port}`);
  
  // Initialize service
  await initializeIcebergService();
  
  console.log(`🔍 Health check: http://localhost:${port}/health`);
  console.log(`📝 Test query: curl -X POST http://localhost:${port}/api/query -H "Content-Type: application/json" -d '{"query": "SELECT COUNT(*) as count FROM logs"}'`);
});

export default app;