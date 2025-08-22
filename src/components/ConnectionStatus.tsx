import React from 'react';
import { Badge } from '@/components/ui/badge';
import { useData } from '@/contexts/DataContext';
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip';

const ConnectionStatus: React.FC = () => {
  const { isBackendConnected, useRealData, setUseRealData } = useData();

  return (
    <TooltipProvider>
      <div className="flex items-center gap-2">
        <Tooltip>
          <TooltipTrigger asChild>
            <Badge 
              variant={isBackendConnected ? "default" : "secondary"}
              className={`cursor-pointer transition-all ${
                isBackendConnected 
                  ? "bg-success/20 text-success border-success/50 hover:bg-success/30" 
                  : "bg-warning/20 text-warning border-warning/50 hover:bg-warning/30"
              }`}
            >
              {isBackendConnected ? "🟢 Lakehouse Connected" : "🟡 Simulation Mode"}
            </Badge>
          </TooltipTrigger>
          <TooltipContent>
            <p>{isBackendConnected 
              ? "Connected to real Kafka + Spark + Delta Lake backend" 
              : "Using simulated data - start Python scripts to connect"}</p>
          </TooltipContent>
        </Tooltip>

        <Tooltip>
          <TooltipTrigger asChild>
            <Badge
              variant="outline"
              className={`cursor-pointer transition-all ${
                useRealData 
                  ? "bg-primary/20 text-primary border-primary/50 hover:bg-primary/30" 
                  : "bg-muted text-muted-foreground"
              }`}
              onClick={() => setUseRealData(!useRealData)}
            >
              {useRealData ? "Real Data" : "Demo Data"}
            </Badge>
          </TooltipTrigger>
          <TooltipContent>
            <p>Click to toggle between real backend data and demo simulation</p>
          </TooltipContent>
        </Tooltip>
      </div>
    </TooltipProvider>
  );
};

export default ConnectionStatus;