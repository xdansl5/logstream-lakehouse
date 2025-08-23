import { useState, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Database, Table as TableIcon, FileText, Code, Info } from "lucide-react";
import { useToast } from "@/hooks/use-toast";

interface TableSchema {
  name: string;
  schema: {
    fields: Array<{
      name: string;
      type: string;
    }>;
  };
}

interface DeltaLakeExplorerProps {
  onQuerySelect: (query: string) => void;
}

const DeltaLakeExplorer = ({ onQuerySelect }: DeltaLakeExplorerProps) => {
  const [tables, setTables] = useState<TableSchema[]>([]);
  const [loading, setLoading] = useState(true);
  const [selectedTable, setSelectedTable] = useState<string | null>(null);
  const { toast } = useToast();

  const serverUrl = (import.meta as any).env?.VITE_SERVER_URL ?? 'http://localhost:4000';

  useEffect(() => {
    loadTables();
  }, []);

  const loadTables = async () => {
    try {
      setLoading(true);
      const response = await fetch(`${serverUrl}/api/tables`);
      
      if (!response.ok) {
        throw new Error('Failed to load tables');
      }

      const data = await response.json();
      setTables(data.tables || []);
    } catch (error) {
      console.error('Error loading tables:', error);
      toast({
        title: "Error",
        description: "Failed to load Delta Lake tables",
        variant: "destructive",
      });
    } finally {
      setLoading(false);
    }
  };

  const getQuerySuggestions = (tableName: string) => {
    const suggestions = [
      {
        name: "Recent Activity",
        query: `SELECT 
  timestamp,
  level,
  source,
  message,
  status,
  response_time
FROM delta_lake.${tableName}
WHERE timestamp >= current_timestamp() - interval 1 hour
ORDER BY timestamp DESC
LIMIT 100`
      },
      {
        name: "Error Analysis",
        query: `SELECT 
  endpoint,
  count(*) as error_count,
  avg(response_time) as avg_response_time,
  max(response_time) as max_response_time
FROM delta_lake.${tableName}
WHERE status >= 400 
  AND timestamp >= current_timestamp() - interval 24 hours
GROUP BY endpoint
ORDER BY error_count DESC`
      },
      {
        name: "Performance Metrics",
        query: `SELECT 
  source,
  count(*) as total_requests,
  avg(response_time) as avg_response_time,
  sum(case when status >= 400 then 1 else 0 end) as errors
FROM delta_lake.${tableName}
WHERE timestamp >= current_timestamp() - interval 1 day
GROUP BY source
ORDER BY total_requests DESC`
      },
      {
        name: "User Session Analysis",
        query: `SELECT 
  user_id,
  count(distinct session_id) as unique_sessions,
  count(*) as total_requests,
  avg(response_time) as avg_session_time
FROM delta_lake.${tableName}
WHERE timestamp >= current_date()
GROUP BY user_id
HAVING total_requests > 10
ORDER BY total_requests DESC
LIMIT 20`
      },
      {
        name: "Anomaly Detection",
        query: `SELECT 
  endpoint,
  source,
  level,
  count(*) as anomaly_count,
  avg(response_time) as avg_response_time
FROM delta_lake.${tableName}
WHERE (level = 'ERROR' OR response_time > 1000)
  AND timestamp >= current_timestamp() - interval 6 hours
GROUP BY endpoint, source, level
ORDER BY anomaly_count DESC`
      },
      {
        name: "Hourly Trends",
        query: `SELECT 
  date_format(timestamp, 'yyyy-MM-dd HH:mm') as hour,
  count(*) as total_requests,
  sum(case when status >= 400 then 1 else 0 end) as errors,
  avg(response_time) as avg_response_time
FROM delta_lake.${tableName}
WHERE timestamp >= current_timestamp() - interval 24 hours
GROUP BY date_format(timestamp, 'yyyy-MM-dd HH:mm')
ORDER BY hour DESC`
      }
    ];

    return suggestions;
  };

  const getTableSchema = (tableName: string) => {
    return tables.find(table => table.name === tableName)?.schema;
  };

  if (loading) {
    return (
      <Card className="border-border/50 bg-card/50 backdrop-blur">
        <CardHeader>
          <CardTitle className="text-lg font-semibold flex items-center space-x-2">
            <Database className="h-5 w-5 text-primary" />
            <span>Delta Lake Explorer</span>
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="flex items-center justify-center py-8">
            <div className="text-muted-foreground">Loading Delta Lake tables...</div>
          </div>
        </CardContent>
      </Card>
    );
  }

  return (
    <Card className="border-border/50 bg-card/50 backdrop-blur">
      <CardHeader>
        <div className="flex items-center justify-between">
          <div>
            <CardTitle className="text-lg font-semibold flex items-center space-x-2">
              <Database className="h-5 w-5 text-primary" />
              <span>Delta Lake Explorer</span>
            </CardTitle>
            <p className="text-sm text-muted-foreground">
              Explore tables and get query suggestions
            </p>
          </div>
          <div className="flex space-x-2">
            <Badge variant="outline" className="bg-success/10 text-success border-success/20">
              <Database className="h-3 w-3 mr-1" />
              Connected
            </Badge>
            <Badge variant="outline">
              {tables.length} Tables
            </Badge>
          </div>
        </div>
      </CardHeader>
      <CardContent className="space-y-6">
        {/* Tables List */}
        <div>
          <h3 className="text-sm font-medium mb-3 flex items-center">
            <TableIcon className="h-4 w-4 mr-2" />
            Available Tables
          </h3>
          <div className="space-y-2">
            {tables.map((table) => (
              <div
                key={table.name}
                className={`p-3 rounded-lg border cursor-pointer transition-colors ${
                  selectedTable === table.name
                    ? 'border-primary bg-primary/5'
                    : 'border-border/50 hover:border-border'
                }`}
                onClick={() => setSelectedTable(selectedTable === table.name ? null : table.name)}
              >
                <div className="flex items-center justify-between">
                  <div className="flex items-center space-x-2">
                    <TableIcon className="h-4 w-4 text-muted-foreground" />
                    <span className="font-mono text-sm">{table.name}</span>
                    <Badge variant="secondary" className="text-xs">
                      {table.schema.fields.length} columns
                    </Badge>
                  </div>
                  <div className="text-xs text-muted-foreground">
                    {table.schema.fields.length} fields
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>

        {/* Table Schema */}
        {selectedTable && (
          <div>
            <h3 className="text-sm font-medium mb-3 flex items-center">
              <FileText className="h-4 w-4 mr-2" />
              Schema: {selectedTable}
            </h3>
            <div className="border border-border/50 rounded-lg overflow-hidden">
              <Table>
                <TableHeader>
                  <TableRow className="border-border/30">
                    <TableHead className="font-mono text-xs">Column</TableHead>
                    <TableHead className="font-mono text-xs">Type</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {getTableSchema(selectedTable)?.fields.map((field) => (
                    <TableRow key={field.name} className="border-border/30">
                      <TableCell className="font-mono text-xs">{field.name}</TableCell>
                      <TableCell className="font-mono text-xs">
                        <Badge variant="outline" className="text-xs">
                          {field.type}
                        </Badge>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </div>
          </div>
        )}

        {/* Query Suggestions */}
        {selectedTable && (
          <div>
            <h3 className="text-sm font-medium mb-3 flex items-center">
              <Code className="h-4 w-4 mr-2" />
              Query Suggestions
            </h3>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-2">
              {getQuerySuggestions(selectedTable).map((suggestion, index) => (
                <Button
                  key={index}
                  variant="outline"
                  size="sm"
                  onClick={() => onQuerySelect(suggestion.query)}
                  className="text-xs h-auto p-3 justify-start text-left"
                >
                  <div>
                    <div className="font-medium">{suggestion.name}</div>
                    <div className="text-muted-foreground text-xs mt-1">
                      Click to load query
                    </div>
                  </div>
                </Button>
              ))}
            </div>
          </div>
        )}

        {/* Info */}
        <div className="bg-muted/30 rounded-lg p-3">
          <div className="flex items-start space-x-2">
            <Info className="h-4 w-4 text-muted-foreground mt-0.5" />
            <div className="text-xs text-muted-foreground">
              <p className="font-medium mb-1">Delta Lake Integration</p>
              <p>
                This interface connects to your Delta Lake database and executes real queries 
                against the streaming data ingested via Spark. All results come from actual 
                Delta Lake tables, not simulated data.
              </p>
            </div>
          </div>
        </div>
      </CardContent>
    </Card>
  );
};

export default DeltaLakeExplorer;