import { useEffect, useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Badge } from "@/components/ui/badge";
import { Play, Pause } from "lucide-react";
import { Button } from "@/components/ui/button";

interface LogEntry {
  id: string;
  timestamp: string;
  level: "INFO" | "WARN" | "ERROR" | "DEBUG";
  source: string;
  message: string;
  ip?: string;
  status?: number;
  responseTime?: number;
}

const LogStream = () => {
  const [logs, setLogs] = useState<LogEntry[]>([]);
  const [isStreaming, setIsStreaming] = useState(true);

  // Mock log generator
  const generateLog = (): LogEntry => {
    const levels: LogEntry["level"][] = ["INFO", "WARN", "ERROR", "DEBUG"];
    const sources = ["web-server", "api-gateway", "database", "auth-service", "kafka-consumer"];
    const messages = [
      "Request processed successfully",
      "Database connection timeout",
      "User authentication failed",
      "Cache miss for key: user_session_",
      "Spark job completed",
      "Delta table compaction started",
      "Anomaly detected in request pattern",
      "Streaming pipeline healthy"
    ];

    const level = levels[Math.floor(Math.random() * levels.length)];
    const isError = level === "ERROR";
    
    return {
      id: Math.random().toString(36).substr(2, 9),
      timestamp: new Date().toISOString(),
      level,
      source: sources[Math.floor(Math.random() * sources.length)],
      message: messages[Math.floor(Math.random() * messages.length)],
      ip: `192.168.1.${Math.floor(Math.random() * 255)}`,
      status: isError ? 500 : Math.random() > 0.8 ? 404 : 200,
      responseTime: Math.floor(Math.random() * 500) + 50
    };
  };

  useEffect(() => {
    if (!isStreaming) return;

    const interval = setInterval(() => {
      const newLog = generateLog();
      setLogs(prev => [newLog, ...prev.slice(0, 99)]); // Keep only last 100 logs
    }, Math.random() * 2000 + 500); // Random interval between 500ms-2.5s

    return () => clearInterval(interval);
  }, [isStreaming]);

  const getLevelColor = (level: LogEntry["level"]) => {
    switch (level) {
      case "ERROR": return "destructive";
      case "WARN": return "outline";
      case "INFO": return "secondary";
      case "DEBUG": return "outline";
      default: return "secondary";
    }
  };

  const formatTimestamp = (timestamp: string) => {
    return new Date(timestamp).toLocaleTimeString();
  };

  return (
    <Card className="border-border/50 bg-card/50 backdrop-blur">
      <CardHeader>
        <div className="flex items-center justify-between">
          <div>
            <CardTitle className="text-lg font-semibold">Live Log Stream</CardTitle>
            <p className="text-sm text-muted-foreground">Real-time log ingestion from Kafka</p>
          </div>
          <Button
            variant={isStreaming ? "default" : "outline"}
            size="sm"
            onClick={() => setIsStreaming(!isStreaming)}
            className="flex items-center space-x-2"
          >
            {isStreaming ? <Pause className="h-4 w-4" /> : <Play className="h-4 w-4" />}
            <span>{isStreaming ? "Pause" : "Resume"}</span>
          </Button>
        </div>
      </CardHeader>
      <CardContent>
        <ScrollArea className="h-[400px] w-full">
          <div className="space-y-2">
            {logs.map((log) => (
              <div
                key={log.id}
                className="flex items-start space-x-3 p-3 rounded-lg bg-muted/30 hover:bg-muted/50 transition-colors border border-border/30"
              >
                <Badge variant={getLevelColor(log.level)} className="text-xs">
                  {log.level}
                </Badge>
                <div className="flex-1 min-w-0">
                  <div className="flex items-center space-x-2 text-xs text-muted-foreground mb-1">
                    <span>{formatTimestamp(log.timestamp)}</span>
                    <span>•</span>
                    <span className="font-mono">{log.source}</span>
                    {log.ip && (
                      <>
                        <span>•</span>
                        <span className="font-mono">{log.ip}</span>
                      </>
                    )}
                    {log.status && (
                      <>
                        <span>•</span>
                        <span className={`font-mono ${log.status >= 400 ? 'text-destructive' : 'text-success'}`}>
                          {log.status}
                        </span>
                      </>
                    )}
                    {log.responseTime && (
                      <>
                        <span>•</span>
                        <span className="font-mono">{log.responseTime}ms</span>
                      </>
                    )}
                  </div>
                  <p className="text-sm text-foreground font-mono break-all">
                    {log.message}
                  </p>
                </div>
              </div>
            ))}
            {logs.length === 0 && (
              <div className="text-center py-8 text-muted-foreground">
                <p>No logs available. Start streaming to see real-time data.</p>
              </div>
            )}
          </div>
        </ScrollArea>
      </CardContent>
    </Card>
  );
};

export default LogStream;