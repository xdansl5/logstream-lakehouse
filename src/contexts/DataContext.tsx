import React, { createContext, useContext, useEffect, useState, useCallback } from 'react';

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

export interface Metric {
  title: string;
  value: string;
  change: string;
  trend: "up" | "down";
  icon: React.ReactNode;
  status: "success" | "warning" | "error" | "info";
}

export interface ChartData {
  time: string;
  requests: number;
  errors: number;
  responseTime: number;
}

export interface QueryResult {
  [key: string]: any;
}

interface DataContextType {
  logs: LogEntry[];
  metrics: Metric[];
  chartData: ChartData[];
  isStreaming: boolean;
  setIsStreaming: (streaming: boolean) => void;
  executeQuery: (query: string) => Promise<{ results: QueryResult[]; executionTime: string }>;
  clearLogs: () => void;
  getAnomalies: () => LogEntry[];
  sseConnected: boolean;
}

const DataContext = createContext<DataContextType | undefined>(undefined);

export const useData = () => {
  const context = useContext(DataContext);
  if (!context) {
    throw new Error('useData must be used within a DataProvider');
  }
  return context;
};

export const DataProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const [logs, setLogs] = useState<LogEntry[]>([]);
  const [chartData, setChartData] = useState<ChartData[]>([]);
  const [isStreaming, setIsStreaming] = useState(true);
  const [currentMetrics, setCurrentMetrics] = useState({
    eventsPerSec: 0,
    errorRate: 0,
    avgResponseTime: 0,
    activeSessions: 0,
    dataProcessed: 0
  });
  const [sseConnected, setSseConnected] = useState(false);

  // Simulate realistic log patterns based on Python scripts
  const generateRealisticLog = useCallback((): LogEntry => {
    const sources = ["spark-streaming", "kafka-consumer", "delta-writer", "web-server", "api-gateway"];
    const endpoints = ["/api/users", "/api/orders", "/api/products", "/health", "/metrics"];
    const userIds = Array.from({ length: 1000 }, (_, i) => `user_${i + 1}`);
    
    const now = new Date();
    const source = sources[Math.floor(Math.random() * sources.length)];
    
    // Create realistic message patterns
    let level: LogEntry["level"] = "INFO";
    let message = "";
    let status = 200;
    let responseTime = Math.floor(Math.random() * 200) + 50;
    
    // Generate anomalies occasionally
    const isAnomaly = Math.random() < 0.05; // 5% anomaly rate
    
    if (isAnomaly) {
      level = "ERROR";
      status = Math.random() < 0.5 ? 500 : 404;
      responseTime = Math.floor(Math.random() * 2000) + 1000;
      message = Math.random() < 0.5 
        ? "Database connection timeout - retrying..."
        : "High error rate detected in endpoint processing";
    } else {
      const patterns = [
        "Spark job processing batch data successfully",
        "Delta Lake table compaction completed",
        "Kafka message consumed and processed",
        "User session analytics updated",
        "Real-time anomaly detection running",
        "Streaming pipeline health check passed"
      ];
      message = patterns[Math.floor(Math.random() * patterns.length)];
      
      if (Math.random() < 0.1) {
        level = "WARN";
        message = "High memory usage detected - monitoring closely";
      }
    }

    return {
      id: Math.random().toString(36).substr(2, 9),
      timestamp: now.toISOString(),
      level,
      source,
      message,
      ip: `192.168.${Math.floor(Math.random() * 10)}.${Math.floor(Math.random() * 255)}`,
      status,
      responseTime,
      endpoint: endpoints[Math.floor(Math.random() * endpoints.length)],
      userId: userIds[Math.floor(Math.random() * userIds.length)],
      sessionId: `sess_${Math.random().toString(36).substr(2, 8)}`
    };
  }, []);

  // Initialize chart data
  useEffect(() => {
    const initialData: ChartData[] = [];
    for (let i = 23; i >= 0; i--) {
      const time = new Date();
      time.setMinutes(time.getMinutes() - i);
      
      initialData.push({
        time: time.toLocaleTimeString('en-US', { 
          hour12: false, 
          hour: '2-digit', 
          minute: '2-digit' 
        }),
        requests: Math.floor(Math.random() * 1000) + 2000,
        errors: Math.floor(Math.random() * 50) + 10,
        responseTime: Math.floor(Math.random() * 100) + 100
      });
    }
    setChartData(initialData);
  }, []);

  // Connect to SSE stream when streaming is enabled
  useEffect(() => {
    if (!isStreaming) return;

    const sseUrl = (import.meta as any).env?.VITE_SSE_URL ?? 'http://localhost:4000/events';
    let es: EventSource | null = null;

    try {
      es = new EventSource(sseUrl, { withCredentials: false });
    } catch (err) {
      setSseConnected(false);
      return;
    }

    es.onopen = () => {
      setSseConnected(true);
    };

    es.onmessage = (event: MessageEvent) => {
      try {
        const data = JSON.parse(event.data);
        if (data && data.type === 'hello') return;
        setLogs(prev => [data as LogEntry, ...prev.slice(0, 999)]);
      } catch (e) {
        // ignore malformed messages
      }
    };

    es.onerror = () => {
      setSseConnected(false);
      if (es) {
        try { es.close(); } catch {}
      }
    };

    return () => {
      setSseConnected(false);
      if (es) {
        try { es.close(); } catch {}
      }
    };
  }, [isStreaming]);

  // Stream logs via simulator only when SSE is not connected
  useEffect(() => {
    if (!isStreaming) return;
    if (sseConnected) return;

    const interval = setInterval(() => {
      const newLog = generateRealisticLog();
      setLogs(prev => [newLog, ...prev.slice(0, 999)]); // Keep last 1000 logs
    }, Math.random() * 1500 + 500); // Between 500ms-2s

    return () => clearInterval(interval);
  }, [isStreaming, generateRealisticLog, sseConnected]);

  // Update chart data in real-time
  useEffect(() => {
    if (!isStreaming) return;

    const interval = setInterval(() => {
      setChartData(prev => {
        const newEntry: ChartData = {
          time: new Date().toLocaleTimeString('en-US', { 
            hour12: false, 
            hour: '2-digit', 
            minute: '2-digit' 
          }),
          requests: Math.floor(Math.random() * 1000) + 2000,
          errors: Math.floor(Math.random() * 50) + 10,
          responseTime: Math.floor(Math.random() * 100) + 100
        };
        
        return [...prev.slice(1), newEntry];
      });
    }, 5000);

    return () => clearInterval(interval);
  }, [isStreaming]);

  // Calculate real-time metrics based on current logs
  useEffect(() => {
    if (logs.length === 0) return;

    const recentLogs = logs.slice(0, 100); // Last 100 logs for metrics
    const errors = recentLogs.filter(log => log.level === "ERROR");
    const totalRequests = recentLogs.length;
    const avgResponseTime = recentLogs.reduce((acc, log) => acc + (log.responseTime || 0), 0) / totalRequests;
    const uniqueSessions = new Set(recentLogs.map(log => log.sessionId)).size;

    setCurrentMetrics({
      eventsPerSec: totalRequests / 10, // Approximate events per second
      errorRate: (errors.length / totalRequests) * 100,
      avgResponseTime: Math.round(avgResponseTime),
      activeSessions: uniqueSessions * 100, // Scale up for demo
      dataProcessed: Math.random() * 100 + 800 // GB processed
    });
  }, [logs]);

  // Execute real Delta Lake queries via API
  const executeQuery = useCallback(async (query: string): Promise<{ results: QueryResult[]; executionTime: string }> => {
    try {
      const serverUrl = (import.meta as any).env?.VITE_SERVER_URL ?? 'http://localhost:4000';
      const response = await fetch(`${serverUrl}/api/query`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ query }),
      });

      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.error || 'Query execution failed');
      }

      const data = await response.json();
      return {
        results: data.results || [],
        executionTime: data.executionTime || '0s'
      };
    } catch (error) {
      console.error('Delta Lake query error:', error);
      throw new Error(`Query execution failed: ${error.message}`);
    }
  }, []);

  const clearLogs = useCallback(() => {
    setLogs([]);
  }, []);

  const getAnomalies = useCallback(() => {
    return logs.filter(log => 
      log.level === "ERROR" || 
      (log.responseTime && log.responseTime > 1000) ||
      log.message.includes("anomaly") ||
      log.message.includes("timeout")
    ).slice(0, 50);
  }, [logs]);

  // Generate metrics based on current data
  const metrics: Metric[] = [
    {
      title: "Events/sec",
      value: Math.round(currentMetrics.eventsPerSec).toLocaleString(),
      change: `+${(Math.random() * 20 + 5).toFixed(1)}%`,
      trend: "up",
      icon: <div>📊</div>,
      status: "success"
    },
    {
      title: "Error Rate",
      value: `${currentMetrics.errorRate.toFixed(2)}%`,
      change: `${currentMetrics.errorRate > 2 ? '+' : '-'}${(Math.random() * 10 + 1).toFixed(1)}%`,
      trend: currentMetrics.errorRate > 2 ? "up" : "down",
      icon: <div>⚠️</div>,
      status: currentMetrics.errorRate > 5 ? "error" : currentMetrics.errorRate > 2 ? "warning" : "success"
    },
    {
      title: "Avg Response Time",
      value: `${currentMetrics.avgResponseTime}ms`,
      change: `${currentMetrics.avgResponseTime > 150 ? '+' : '-'}${(Math.random() * 10 + 2).toFixed(1)}%`,
      trend: currentMetrics.avgResponseTime > 150 ? "up" : "down",
      icon: <div>⏱️</div>,
      status: currentMetrics.avgResponseTime > 200 ? "warning" : "success"
    },
    {
      title: "Active Sessions",
      value: currentMetrics.activeSessions.toLocaleString(),
      change: `+${(Math.random() * 15 + 3).toFixed(1)}%`,
      trend: "up",
      icon: <div>👥</div>,
      status: "info"
    },
    {
      title: "Data Processed",
      value: `${currentMetrics.dataProcessed.toFixed(0)} GB`,
      change: `+${(Math.random() * 20 + 10).toFixed(1)}%`,
      trend: "up",
      icon: <div>💾</div>,
      status: "success"
    },
    {
      title: "Delta Tables",
      value: "23",
      change: "0%",
      trend: "up",
      icon: <div>🏛️</div>,
      status: "info"
    }
  ];

  return (
    <DataContext.Provider value={{
      logs,
      metrics,
      chartData,
      isStreaming,
      setIsStreaming,
      executeQuery,
      clearLogs,
      getAnomalies,
      sseConnected
    }}>
      {children}
    </DataContext.Provider>
  );
};