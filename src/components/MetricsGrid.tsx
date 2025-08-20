import { useEffect, useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { TrendingUp, TrendingDown, AlertTriangle, CheckCircle, Clock, Users } from "lucide-react";

interface Metric {
  title: string;
  value: string;
  change: string;
  trend: "up" | "down";
  icon: React.ReactNode;
  status: "success" | "warning" | "error" | "info";
}

const MetricsGrid = () => {
  const [metrics, setMetrics] = useState<Metric[]>([
    {
      title: "Events/sec",
      value: "2,847",
      change: "+12.5%",
      trend: "up",
      icon: <TrendingUp className="h-4 w-4" />,
      status: "success"
    },
    {
      title: "Error Rate",
      value: "0.34%",
      change: "-23.1%",
      trend: "down",
      icon: <AlertTriangle className="h-4 w-4" />,
      status: "success"
    },
    {
      title: "Avg Response Time",
      value: "145ms",
      change: "+5.2%",
      trend: "up",
      icon: <Clock className="h-4 w-4" />,
      status: "warning"
    },
    {
      title: "Active Sessions",
      value: "18,429",
      change: "+8.7%",
      trend: "up",
      icon: <Users className="h-4 w-4" />,
      status: "info"
    },
    {
      title: "Data Processed",
      value: "847 GB",
      change: "+15.3%",
      trend: "up",
      icon: <CheckCircle className="h-4 w-4" />,
      status: "success"
    },
    {
      title: "Delta Tables",
      value: "23",
      change: "0%",
      trend: "up",
      icon: <TrendingUp className="h-4 w-4" />,
      status: "info"
    }
  ]);

  // Simulate real-time updates
  useEffect(() => {
    const interval = setInterval(() => {
      setMetrics(prev => prev.map(metric => ({
        ...metric,
        value: metric.title === "Events/sec" 
          ? (Math.floor(Math.random() * 1000) + 2000).toLocaleString()
          : metric.value
      })));
    }, 3000);

    return () => clearInterval(interval);
  }, []);

  const getStatusColor = (status: string) => {
    switch (status) {
      case "success": return "text-success";
      case "warning": return "text-warning";
      case "error": return "text-destructive";
      default: return "text-primary";
    }
  };

  const getTrendColor = (trend: string) => {
    return trend === "up" ? "text-success" : "text-destructive";
  };

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
      {metrics.map((metric, index) => (
        <Card key={index} className="group hover:shadow-glow transition-all duration-300 border-border/50 bg-card/50 backdrop-blur">
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium text-muted-foreground">
              {metric.title}
            </CardTitle>
            <div className={getStatusColor(metric.status)}>
              {metric.icon}
            </div>
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold text-foreground mb-1">
              {metric.value}
            </div>
            <div className="flex items-center text-sm">
              {metric.trend === "up" ? (
                <TrendingUp className="h-3 w-3 mr-1 text-success" />
              ) : (
                <TrendingDown className="h-3 w-3 mr-1 text-destructive" />
              )}
              <span className={getTrendColor(metric.trend)}>
                {metric.change}
              </span>
              <span className="text-muted-foreground ml-1">from last hour</span>
            </div>
          </CardContent>
        </Card>
      ))}
    </div>
  );
};

export default MetricsGrid;