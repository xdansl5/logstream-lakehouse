import { useEffect, useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, AreaChart, Area } from "recharts";

interface ChartData {
  time: string;
  requests: number;
  errors: number;
  responseTime: number;
}

const AnalyticsChart = () => {
  const [data, setData] = useState<ChartData[]>([]);

  // Generate initial data
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
    setData(initialData);
  }, []);

  // Update data in real-time
  useEffect(() => {
    const interval = setInterval(() => {
      setData(prev => {
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
  }, []);

  return (
    <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
      <Card className="border-border/50 bg-card/50 backdrop-blur">
        <CardHeader>
          <CardTitle className="text-lg font-semibold">Request Volume</CardTitle>
          <p className="text-sm text-muted-foreground">Real-time request processing</p>
        </CardHeader>
        <CardContent>
          <ResponsiveContainer width="100%" height={250}>
            <AreaChart data={data}>
              <defs>
                <linearGradient id="requestGradient" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="hsl(var(--primary))" stopOpacity={0.8}/>
                  <stop offset="95%" stopColor="hsl(var(--primary))" stopOpacity={0.1}/>
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border))" />
              <XAxis 
                dataKey="time" 
                stroke="hsl(var(--muted-foreground))" 
                fontSize={12}
              />
              <YAxis 
                stroke="hsl(var(--muted-foreground))" 
                fontSize={12}
              />
              <Tooltip 
                contentStyle={{
                  backgroundColor: "hsl(var(--card))",
                  border: "1px solid hsl(var(--border))",
                  borderRadius: "8px"
                }}
              />
              <Area
                type="monotone"
                dataKey="requests"
                stroke="hsl(var(--primary))"
                fillOpacity={1}
                fill="url(#requestGradient)"
                strokeWidth={2}
              />
            </AreaChart>
          </ResponsiveContainer>
        </CardContent>
      </Card>

      <Card className="border-border/50 bg-card/50 backdrop-blur">
        <CardHeader>
          <CardTitle className="text-lg font-semibold">Error Rate & Response Time</CardTitle>
          <p className="text-sm text-muted-foreground">Performance monitoring</p>
        </CardHeader>
        <CardContent>
          <ResponsiveContainer width="100%" height={250}>
            <LineChart data={data}>
              <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border))" />
              <XAxis 
                dataKey="time" 
                stroke="hsl(var(--muted-foreground))" 
                fontSize={12}
              />
              <YAxis 
                yAxisId="left"
                stroke="hsl(var(--muted-foreground))" 
                fontSize={12}
              />
              <YAxis 
                yAxisId="right"
                orientation="right"
                stroke="hsl(var(--muted-foreground))" 
                fontSize={12}
              />
              <Tooltip 
                contentStyle={{
                  backgroundColor: "hsl(var(--card))",
                  border: "1px solid hsl(var(--border))",
                  borderRadius: "8px"
                }}
              />
              <Line
                yAxisId="left"
                type="monotone"
                dataKey="errors"
                stroke="hsl(var(--destructive))"
                strokeWidth={2}
                dot={{ fill: "hsl(var(--destructive))", strokeWidth: 2, r: 3 }}
                name="Errors"
              />
              <Line
                yAxisId="right"
                type="monotone"
                dataKey="responseTime"
                stroke="hsl(var(--warning))"
                strokeWidth={2}
                dot={{ fill: "hsl(var(--warning))", strokeWidth: 2, r: 3 }}
                name="Response Time (ms)"
              />
            </LineChart>
          </ResponsiveContainer>
        </CardContent>
      </Card>
    </div>
  );
};

export default AnalyticsChart;