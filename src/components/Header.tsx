import { Activity, Database, Zap } from "lucide-react";

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
            <div className="flex items-center space-x-2 text-sm">
              <Activity className="h-4 w-4 text-success" />
              <span className="text-muted-foreground">Streaming</span>
              <div className="h-2 w-2 bg-success rounded-full animate-pulse" />
            </div>
            
            <div className="flex items-center space-x-2 text-sm">
              <Zap className="h-4 w-4 text-warning" />
              <span className="text-muted-foreground">Delta Lake</span>
              <span className="text-warning font-medium">Active</span>
            </div>
          </div>
        </div>
      </div>
    </header>
  );
};

export default Header;