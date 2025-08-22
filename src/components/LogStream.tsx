import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Badge } from "@/components/ui/badge";
import { Play, Pause, Trash2, AlertTriangle } from "lucide-react";
import { Button } from "@/components/ui/button";
import { useData } from "@/contexts/DataContext";

const LogStream = () => {
  const { logs, isStreaming, setIsStreaming, clearLogs, getAnomalies } = useData();

  const getLevelColor = (level: string) => {
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
            <p className="text-sm text-muted-foreground">
              Real-time log ingestion from Kafka • {logs.length} events • {getAnomalies().length} anomalies
            </p>
          </div>
          <div className="flex space-x-2">
            <Button
              variant="outline"
              size="sm"
              onClick={clearLogs}
              className="flex items-center space-x-2"
            >
              <Trash2 className="h-4 w-4" />
              <span>Clear</span>
            </Button>
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
                    {log.endpoint && (
                      <>
                        <span>•</span>
                        <span className="font-mono text-primary">{log.endpoint}</span>
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
                        <span className={`font-mono ${log.responseTime > 1000 ? 'text-destructive' : 'text-foreground'}`}>
                          {log.responseTime}ms
                        </span>
                      </>
                    )}
                  </div>
                  <div className="flex items-center justify-between">
                    <p className="text-sm text-foreground font-mono break-all flex-1">
                      {log.message}
                    </p>
                    {(log.level === "ERROR" || (log.responseTime && log.responseTime > 1000)) && (
                      <AlertTriangle className="h-4 w-4 text-destructive ml-2 flex-shrink-0" />
                    )}
                  </div>
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