import { Toaster } from "@/components/ui/toaster";
import { Toaster as Sonner } from "@/components/ui/sonner";
import { TooltipProvider } from "@/components/ui/tooltip";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { BrowserRouter, Routes, Route } from "react-router-dom";
import { DataProvider } from "./contexts/DataContext";
import BackendStatus from "./components/BackendStatus";

const queryClient = new QueryClient();

const App = () => (
  <QueryClientProvider client={queryClient}>
    <TooltipProvider>
      <DataProvider>
        <Toaster />
        <Sonner />
        <BrowserRouter>
          <div className="min-h-screen bg-background">
            <main className="container mx-auto px-6 py-8 space-y-8">
              <div className="mb-6">
                <h1 className="text-3xl font-bold text-foreground">🧪 Iceberg Analytics Test</h1>
                <p className="text-muted-foreground">Test di connessione al backend</p>
              </div>
              
              {/* Backend Status */}
              <section>
                <div className="mb-6">
                  <h2 className="text-2xl font-bold text-foreground">Backend Status</h2>
                  <p className="text-muted-foreground">Monitora la connessione al server Iceberg</p>
                </div>
                <BackendStatus />
              </section>
            </main>
          </div>
        </BrowserRouter>
      </DataProvider>
    </TooltipProvider>
  </QueryClientProvider>
);

export default App;
