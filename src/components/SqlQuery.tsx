import { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Textarea } from "@/components/ui/textarea";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Play, Database, Clock } from "lucide-react";
import { Badge } from "@/components/ui/badge";

const SqlQuery = () => {
  const [query, setQuery] = useState(
    `SELECT 
  date_format(timestamp, 'yyyy-MM-dd HH:mm') as hour,
  count(*) as total_requests,
  sum(case when status_code >= 400 then 1 else 0 end) as errors,
  avg(response_time) as avg_response_time
FROM delta_lake.logs 
WHERE timestamp >= current_timestamp() - interval 1 day
GROUP BY date_format(timestamp, 'yyyy-MM-dd HH:mm')
ORDER BY hour DESC
LIMIT 10`
  );
  
  const [isRunning, setIsRunning] = useState(false);
  const [results, setResults] = useState<any[]>([]);
  const [executionTime, setExecutionTime] = useState<string | null>(null);

  const mockResults = [
    { hour: "2024-01-15 14:30", total_requests: 2847, errors: 12, avg_response_time: 145.3 },
    { hour: "2024-01-15 14:29", total_requests: 2756, errors: 8, avg_response_time: 132.1 },
    { hour: "2024-01-15 14:28", total_requests: 2912, errors: 15, avg_response_time: 158.7 },
    { hour: "2024-01-15 14:27", total_requests: 2634, errors: 6, avg_response_time: 127.9 },
    { hour: "2024-01-15 14:26", total_requests: 2789, errors: 11, avg_response_time: 141.2 }
  ];

  const handleRunQuery = async () => {
    setIsRunning(true);
    setExecutionTime(null);
    
    // Simulate query execution
    await new Promise(resolve => setTimeout(resolve, 1500));
    
    setResults(mockResults);
    setExecutionTime("1.23s");
    setIsRunning(false);
  };

  const sampleQueries = [
    {
      name: "Top Error Endpoints",
      query: `SELECT endpoint, count(*) as error_count
FROM delta_lake.logs 
WHERE status_code >= 400 AND timestamp >= current_timestamp() - interval 1 hour
GROUP BY endpoint
ORDER BY error_count DESC
LIMIT 5`
    },
    {
      name: "User Session Analysis",
      query: `SELECT 
  user_id,
  count(distinct session_id) as sessions,
  sum(case when event_type = 'page_view' then 1 else 0 end) as page_views
FROM delta_lake.user_events
WHERE date = current_date()
GROUP BY user_id
ORDER BY page_views DESC`
    }
  ];

  return (
    <Card className="border-border/50 bg-card/50 backdrop-blur">
      <CardHeader>
        <div className="flex items-center justify-between">
          <div>
            <CardTitle className="text-lg font-semibold flex items-center space-x-2">
              <Database className="h-5 w-5 text-primary" />
              <span>Spark SQL Query Interface</span>
            </CardTitle>
            <p className="text-sm text-muted-foreground">Interactive analytics on Delta Lake</p>
          </div>
          <Badge variant="outline" className="bg-gradient-primary text-primary-foreground border-primary/20">
            Delta Lake Connected
          </Badge>
        </div>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="space-y-2">
          <div className="flex space-x-2 mb-2">
            {sampleQueries.map((sample, index) => (
              <Button
                key={index}
                variant="outline"
                size="sm"
                onClick={() => setQuery(sample.query)}
                className="text-xs"
              >
                {sample.name}
              </Button>
            ))}
          </div>
          
          <Textarea
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            placeholder="Enter your Spark SQL query..."
            className="min-h-[120px] font-mono text-sm bg-muted/30 border-border/50"
          />
          
          <div className="flex items-center justify-between">
            <Button 
              onClick={handleRunQuery}
              disabled={isRunning}
              className="flex items-center space-x-2"
            >
              <Play className="h-4 w-4" />
              <span>{isRunning ? "Running..." : "Execute Query"}</span>
            </Button>
            
            {executionTime && (
              <div className="flex items-center space-x-2 text-sm text-muted-foreground">
                <Clock className="h-4 w-4" />
                <span>Executed in {executionTime}</span>
              </div>
            )}
          </div>
        </div>

        {results.length > 0 && (
          <div className="border border-border/50 rounded-lg bg-muted/20">
            <div className="p-3 border-b border-border/50 bg-muted/30">
              <h4 className="text-sm font-medium">Query Results ({results.length} rows)</h4>
            </div>
            <div className="overflow-x-auto">
              <Table>
                <TableHeader>
                  <TableRow className="border-border/30">
                    <TableHead className="font-mono text-xs">hour</TableHead>
                    <TableHead className="font-mono text-xs">total_requests</TableHead>
                    <TableHead className="font-mono text-xs">errors</TableHead>
                    <TableHead className="font-mono text-xs">avg_response_time</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {results.map((row, index) => (
                    <TableRow key={index} className="border-border/30">
                      <TableCell className="font-mono text-xs">{row.hour}</TableCell>
                      <TableCell className="font-mono text-xs">{row.total_requests}</TableCell>
                      <TableCell className="font-mono text-xs">
                        <span className={row.errors > 10 ? "text-destructive" : "text-foreground"}>
                          {row.errors}
                        </span>
                      </TableCell>
                      <TableCell className="font-mono text-xs">
                        {row.avg_response_time.toFixed(1)}ms
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </div>
          </div>
        )}
      </CardContent>
    </Card>
  );
};

export default SqlQuery;