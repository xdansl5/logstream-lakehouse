import React, { useState, useEffect } from 'react';
import { apiService } from '../services/apiService';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { Loader2, CheckCircle, XCircle, Database, Activity } from 'lucide-react';

interface BackendStatus {
  status: 'checking' | 'healthy' | 'unhealthy' | 'error';
  message: string;
  details?: any;
}

interface TableInfo {
  name: string;
  type: string;
  recordCount: number | string;
  lastUpdated: string;
  status: string;
}

export const BackendStatus: React.FC = () => {
  const [backendStatus, setBackendStatus] = useState<BackendStatus>({
    status: 'checking',
    message: 'Checking backend connection...'
  });
  
  const [tables, setTables] = useState<TableInfo[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [queryResult, setQueryResult] = useState<any>(null);

  // Check backend health on component mount
  useEffect(() => {
    checkBackendHealth();
  }, []);

  const checkBackendHealth = async () => {
    try {
      setBackendStatus({
        status: 'checking',
        message: 'Checking backend connection...'
      });

      const health = await apiService.checkHealth();
      
      setBackendStatus({
        status: 'healthy',
        message: 'Backend is healthy and responding',
        details: health
      });

      // Load tables if backend is healthy
      await loadTables();
      
    } catch (error) {
      setBackendStatus({
        status: 'error',
        message: 'Failed to connect to backend',
        details: error instanceof Error ? error.message : 'Unknown error'
      });
    }
  };

  const loadTables = async () => {
    try {
      const tablesData = await apiService.getTables();
      if (tablesData.success) {
        setTables(tablesData.tables);
      }
    } catch (error) {
      console.error('Failed to load tables:', error);
    }
  };

  const testQuery = async () => {
    try {
      setIsLoading(true);
      const result = await apiService.executeQuery('SELECT COUNT(*) as count FROM logs');
      setQueryResult(result);
    } catch (error) {
      setQueryResult({
        error: error instanceof Error ? error.message : 'Unknown error'
      });
    } finally {
      setIsLoading(false);
    }
  };

  const getStatusIcon = () => {
    switch (backendStatus.status) {
      case 'checking':
        return <Loader2 className="h-4 w-4 animate-spin" />;
      case 'healthy':
        return <CheckCircle className="h-4 w-4 text-green-500" />;
      case 'unhealthy':
      case 'error':
        return <XCircle className="h-4 w-4 text-red-500" />;
      default:
        return <Activity className="h-4 w-4" />;
    }
  };

  const getStatusColor = () => {
    switch (backendStatus.status) {
      case 'healthy':
        return 'bg-green-100 text-green-800 border-green-200';
      case 'error':
        return 'bg-red-100 text-red-800 border-red-200';
      case 'checking':
        return 'bg-yellow-100 text-yellow-800 border-yellow-200';
      default:
        return 'bg-gray-100 text-gray-800 border-gray-200';
    }
  };

  return (
    <div className="space-y-6">
      {/* Backend Status Card */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Database className="h-5 w-5" />
            Backend Status
          </CardTitle>
          <CardDescription>
            Connection status and health information for the Iceberg Analytics Server
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-2">
              {getStatusIcon()}
              <span className="font-medium">Status</span>
            </div>
            <Badge className={getStatusColor()}>
              {backendStatus.status.toUpperCase()}
            </Badge>
          </div>
          
          <p className="text-sm text-gray-600">
            {backendStatus.message}
          </p>
          
          {backendStatus.details && (
            <div className="text-xs bg-gray-50 p-3 rounded-md">
              <pre className="whitespace-pre-wrap">
                {JSON.stringify(backendStatus.details, null, 2)}
              </pre>
            </div>
          )}
          
          <Button 
            onClick={checkBackendHealth}
            disabled={backendStatus.status === 'checking'}
            variant="outline"
            size="sm"
          >
            Refresh Status
          </Button>
        </CardContent>
      </Card>

      {/* Tables Information */}
      {tables.length > 0 && (
        <Card>
          <CardHeader>
            <CardTitle>Available Tables</CardTitle>
            <CardDescription>
              Iceberg tables in the analytics backend
            </CardDescription>
          </CardHeader>
          <CardContent>
            <div className="space-y-3">
              {tables.map((table, index) => (
                <div key={index} className="flex items-center justify-between p-3 border rounded-lg">
                  <div>
                    <h4 className="font-medium">{table.name}</h4>
                    <p className="text-sm text-gray-500">
                      Type: {table.type} | Records: {table.recordCount}
                    </p>
                  </div>
                  <Badge variant={table.status === 'available' ? 'default' : 'secondary'}>
                    {table.status}
                  </Badge>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      )}

      {/* Test Query Section */}
      <Card>
        <CardHeader>
          <CardTitle>Test Query</CardTitle>
          <CardDescription>
            Test the backend with a simple SQL query
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <Button 
            onClick={testQuery}
            disabled={isLoading || backendStatus.status !== 'healthy'}
          >
            {isLoading ? (
              <>
                <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                Executing...
              </>
            ) : (
              'Execute: SELECT COUNT(*) FROM logs'
            )}
          </Button>
          
          {queryResult && (
            <div className="space-y-2">
              <h4 className="font-medium">Query Result:</h4>
              {queryResult.error ? (
                <Alert variant="destructive">
                  <AlertDescription>{queryResult.error}</AlertDescription>
                </Alert>
              ) : (
                <div className="bg-gray-50 p-3 rounded-md">
                  <pre className="text-xs whitespace-pre-wrap">
                    {JSON.stringify(queryResult, null, 2)}
                  </pre>
                </div>
              )}
            </div>
          )}
        </CardContent>
      </Card>

      {/* Connection Instructions */}
      {backendStatus.status === 'error' && (
        <Alert>
          <AlertDescription>
            <strong>Backend Connection Failed</strong>
            <br />
            Make sure the Iceberg Analytics Server is running on port 3001.
            <br />
            <code className="mt-2 block bg-gray-100 p-2 rounded">
              cd server && npm start
            </code>
          </AlertDescription>
        </Alert>
      )}
    </div>
  );
};

export default BackendStatus;