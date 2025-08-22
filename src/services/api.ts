/**
 * API Client per comunicare con il backend Lakehouse
 */

const API_BASE_URL = 'http://localhost:8000';

export interface QueryRequest {
  query: string;
}

export interface QueryResult {
  results: Record<string, any>[];
  executionTime: string;
  rowCount: number;
  error?: string;
}

export interface LogEntry {
  id: string;
  timestamp: string;
  level: "INFO" | "WARN" | "ERROR" | "DEBUG";
  source: string;
  message: string;
  ip?: string;
  status?: number;
  responseTime?: number;
  endpoint?: string;
  userId?: string;
  sessionId?: string;
}

export interface MetricsData {
  eventsPerSec: number;
  errorRate: number;
  avgResponseTime: number;
  activeSessions: number;
  dataProcessed: number;
}

class ApiService {
  private baseUrl: string;

  constructor(baseUrl: string = API_BASE_URL) {
    this.baseUrl = baseUrl;
  }

  private async request<T>(endpoint: string, options: RequestInit = {}): Promise<T> {
    const url = `${this.baseUrl}${endpoint}`;
    
    try {
      const response = await fetch(url, {
        headers: {
          'Content-Type': 'application/json',
          ...options.headers,
        },
        ...options,
      });

      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }

      const data = await response.json();
      return data;
    } catch (error) {
      console.error(`❌ API request failed for ${endpoint}:`, error);
      throw error;
    }
  }

  /**
   * Controlla lo stato di salute del backend
   */
  async healthCheck(): Promise<{ status: string; timestamp: string }> {
    return this.request('/api/health');
  }

  /**
   * Ottiene i log correnti dal backend
   */
  async getLogs(): Promise<{ logs: LogEntry[] }> {
    return this.request('/api/logs');
  }

  /**
   * Esegue una query SQL sui dati Delta Lake
   */
  async executeQuery(query: string): Promise<QueryResult> {
    return this.request('/api/query', {
      method: 'POST',
      body: JSON.stringify({ query }),
    });
  }

  /**
   * Ottiene le metriche in tempo reale
   */
  async getMetrics(): Promise<{ metrics: MetricsData }> {
    return this.request('/api/metrics');
  }

  /**
   * Ottiene le anomalie rilevate
   */
  async getAnomalies(): Promise<{ anomalies: LogEntry[] }> {
    return this.request('/api/anomalies');
  }

  /**
   * Testa la connessione al backend
   */
  async testConnection(): Promise<boolean> {
    try {
      await this.healthCheck();
      return true;
    } catch (error) {
      console.error('❌ Backend connection failed:', error);
      return false;
    }
  }
}

export const apiService = new ApiService();

import React from 'react';

/**
 * Hook per verificare lo stato della connessione API
 */
export const useApiConnection = () => {
  const [isConnected, setIsConnected] = React.useState<boolean | null>(null);
  const [lastCheck, setLastCheck] = React.useState<Date | null>(null);

  const checkConnection = React.useCallback(async () => {
    try {
      const connected = await apiService.testConnection();
      setIsConnected(connected);
      setLastCheck(new Date());
      return connected;
    } catch (error) {
      setIsConnected(false);
      setLastCheck(new Date());
      return false;
    }
  }, []);

  React.useEffect(() => {
    // Check connection on mount
    checkConnection();

    // Check every 30 seconds
    const interval = setInterval(checkConnection, 30000);
    
    return () => clearInterval(interval);
  }, [checkConnection]);

  return {
    isConnected,
    lastCheck,
    checkConnection,
  };
};

export default apiService;