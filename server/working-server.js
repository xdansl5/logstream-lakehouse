import express from 'express';
import cors from 'cors';
import IcebergService from './icebergService.js';

const app = express();
const port = 3001;

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

// List available tables
app.get('/api/tables', async (req, res) => {
  try {
    if (!icebergService) {
      return res.status(503).json({ 
        error: 'Iceberg service not available',
        message: 'Service is still initializing or failed to initialize'
      });
    }

    // Get actual table statistics
    const stats = await icebergService.getTableStats('logs');
    
    res.json({
      success: true,
      tables: [
        {
          name: 'logs',
          type: 'file-based',
          recordCount: stats ? stats.total_records : 'unknown',
          lastUpdated: stats ? stats.latest_record : new Date().toISOString(),
          status: icebergService ? 'available' : 'initializing',
          schema: {
            columns: [
              'id', 'timestamp', 'level', 'source', 'message', 
              'ip', 'status', 'response_time', 'endpoint', 
              'user_id', 'session_id', 'method', 'user_agent', 
              'bytes_sent', 'referer'
            ]
          }
        }
      ]
    });
  } catch (error) {
    console.error('Table listing error:', error);
    res.status(500).json({ 
      error: 'Failed to list tables', 
      details: error.message 
    });
  }
});

// Start server with error handling
const server = app.listen(port, async () => {
  console.log(`🚀 Iceberg Analytics Server running on port ${port}`);
  
  // Initialize service after server starts
  await initializeIcebergService();
  
  console.log(`🔍 Health check: http://localhost:${port}/health`);
  console.log(`📝 API docs: http://localhost:${port}/api/tables`);
  console.log(`🌐 CORS enabled for cross-origin requests`);
});

// Handle server errors
server.on('error', (error) => {
  console.error('Server error:', error);
  process.exit(1);
});

// Handle process termination
process.on('SIGTERM', async () => {
  console.log('SIGTERM received, shutting down gracefully');
  server.close(async () => {
    if (icebergService) {
      await icebergService.close();
    }
    process.exit(0);
  });
});

process.on('SIGINT', async () => {
  console.log('SIGINT received, shutting down gracefully');
  server.close(async () => {
    if (icebergService) {
      await icebergService.close();
    }
    process.exit(0);
  });
});

export default app;