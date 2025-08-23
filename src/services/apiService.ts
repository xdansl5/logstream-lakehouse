import API_CONFIG, { buildApiUrl } from '../config/api';

// API Service for Iceberg Analytics Backend
export class ApiService {
  private static instance: ApiService;
  
  private constructor() {}
  
  public static getInstance(): ApiService {
    if (!ApiService.instance) {
      ApiService.instance = new ApiService();
    }
    return ApiService.instance;
  }
  
  // Generic request method with error handling
  private async request<T>(
    endpoint: string, 
    options: RequestInit = {}
  ): Promise<T> {
    const url = buildApiUrl(endpoint);
    const config: RequestInit = {
      ...API_CONFIG.REQUEST_CONFIG,
      ...options,
      headers: {
        ...API_CONFIG.REQUEST_CONFIG.headers,
        ...options.headers,
      },
    };
    
    try {
      const response = await fetch(url, config);
      
      if (!response.ok) {
        const errorData = await response.json().catch(() => ({}));
        throw new Error(errorData.message || `HTTP ${response.status}: ${response.statusText}`);
      }
      
      return await response.json();
    } catch (error) {
      console.error(`API request failed for ${endpoint}:`, error);
      throw error;
    }
  }
  
  // Health check
  async checkHealth(): Promise<any> {
    return this.request(API_CONFIG.ENDPOINTS.HEALTH);
  }
  
  // Execute SQL query
  async executeQuery(query: string): Promise<any> {
    return this.request(API_CONFIG.ENDPOINTS.QUERY, {
      method: 'POST',
      body: JSON.stringify({ query }),
    });
  }
  
  // Get available tables
  async getTables(): Promise<any> {
    return this.request(API_CONFIG.ENDPOINTS.TABLES);
  }
  
  // Get table schema
  async getTableSchema(tableName: string): Promise<any> {
    return this.request(API_CONFIG.ENDPOINTS.TABLE_SCHEMA(tableName));
  }
  
  // Get table data
  async getTableData(tableName: string, limit: number = 100): Promise<any> {
    const endpoint = `${API_CONFIG.ENDPOINTS.TABLE_DATA(tableName)}?limit=${limit}`;
    return this.request(endpoint);
  }
  
  // Get table statistics
  async getTableStats(tableName: string): Promise<any> {
    return this.request(API_CONFIG.ENDPOINTS.TABLE_STATS(tableName));
  }
  
  // Ingest data
  async ingestData(data: any[]): Promise<any> {
    return this.request(API_CONFIG.ENDPOINTS.INGEST, {
      method: 'POST',
      body: JSON.stringify({ data }),
    });
  }
  
  // Server-Sent Events connection
  connectToEvents(): EventSource {
    const url = buildApiUrl(API_CONFIG.ENDPOINTS.EVENTS);
    return new EventSource(url);
  }
}

// Export singleton instance
export const apiService = ApiService.getInstance();

// Export default
export default apiService;