// API Configuration for Iceberg Analytics Backend
export const API_CONFIG = {
  // Backend server URL
  BASE_URL: import.meta.env.VITE_API_BASE_URL || 'http://localhost:3001',
  
  // API endpoints
  ENDPOINTS: {
    HEALTH: '/health',
    QUERY: '/api/query',
    TABLES: '/api/tables',
    TABLE_SCHEMA: (tableName: string) => `/api/tables/${tableName}/schema`,
    TABLE_DATA: (tableName: string) => `/api/tables/${tableName}/data`,
    TABLE_STATS: (tableName: string) => `/api/tables/${tableName}/stats`,
    INGEST: '/api/ingest',
    EVENTS: '/events'
  },
  
  // Request configuration
  REQUEST_CONFIG: {
    headers: {
      'Content-Type': 'application/json',
    },
    timeout: 30000, // 30 seconds
  },
  
  // Retry configuration
  RETRY_CONFIG: {
    maxRetries: 3,
    retryDelay: 1000, // 1 second
  }
};

// Helper function to build full API URLs
export const buildApiUrl = (endpoint: string): string => {
  return `${API_CONFIG.BASE_URL}${endpoint}`;
};

// Helper function to check if backend is available
export const checkBackendHealth = async (): Promise<boolean> => {
  try {
    const response = await fetch(buildApiUrl(API_CONFIG.ENDPOINTS.HEALTH));
    return response.ok;
  } catch (error) {
    console.error('Backend health check failed:', error);
    return false;
  }
};

// Default export
export default API_CONFIG;