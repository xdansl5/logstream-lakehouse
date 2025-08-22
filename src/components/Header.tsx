import { Activity, Database, Zap } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import ConnectionStatus from "./ConnectionStatus";

const Header = () => {
  return (
    <header className="border-b border-border bg-card/50 backdrop-blur supports-[backdrop-filter]:bg-card/50">
      <div className="container mx-auto px-6 py-4">
        <div className="flex items-center justify-between">
          <div className="flex items-center space-x-4">
            <div className="flex items-center space-x-2">
              <div className="relative">
                <Database className="h-8 w-8 text-primary" />
                <div className="absolute -top-1 -right-1 h-3 w-3 bg-success rounded-full animate-pulse" />
              </div>
              <div>
                <h1 className="text-2xl font-bold bg-gradient-primary bg-clip-text text-transparent">
                  LogStream Lakehouse
                </h1>
                <p className="text-sm text-muted-foreground">Real-time Analytics Platform</p>
              </div>
            </div>
          </div>
          
          <div className="flex items-center space-x-6">
            <ConnectionStatus />
            
            <div className="flex items-center gap-2">
              <Badge variant="default" className="bg-success/20 text-success border-success/50">
                <Activity className="w-3 h-3 mr-1" />
                Real-time
              </Badge>
              <Badge variant="secondary" className="bg-primary/20 text-primary border-primary/50">
                <Database className="w-3 h-3 mr-1" />
                Delta Lake
              </Badge>
              <Badge variant="secondary" className="bg-accent/20 text-accent border-accent/50">
                <Zap className="w-3 h-3 mr-1" />
                Spark Streaming
              </Badge>
            </div>
          </div>
        </div>
      </div>
    </header>
  );
};

export default Header;