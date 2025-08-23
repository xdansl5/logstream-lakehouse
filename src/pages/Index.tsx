import Header from "@/components/Header";
import MetricsGrid from "@/components/MetricsGrid";
import LogStream from "@/components/LogStream";
import AnalyticsChart from "@/components/AnalyticsChart";
import SqlQuery from "@/components/SqlQuery";
import DeltaLakeExplorer from "@/components/DeltaLakeExplorer";
import BackendStatus from "@/components/BackendStatus";

const Index = () => {
  return (
    <div className="min-h-screen bg-background">
      <Header />
      
      <main className="container mx-auto px-6 py-8 space-y-8">
        {/* Backend Status */}
        <section>
          <div className="mb-6">
            <h2 className="text-2xl font-bold text-foreground">Backend Status</h2>
            <p className="text-muted-foreground">Monitor the Iceberg Analytics Server connection</p>
          </div>
          <BackendStatus />
        </section>

        {/* Metrics Overview */}
        <section>
          <div className="mb-6">
            <h2 className="text-2xl font-bold text-foreground">Platform Overview</h2>
            <p className="text-muted-foreground">Real-time metrics from your Lakehouse architecture</p>
          </div>
          <MetricsGrid />
        </section>

        {/* Analytics Charts */}
        <section>
          <div className="mb-6">
            <h2 className="text-2xl font-bold text-foreground">Analytics Dashboard</h2>
            <p className="text-muted-foreground">Spark Structured Streaming insights</p>
          </div>
          <AnalyticsChart />
        </section>

        {/* Live Data Stream */}
        <section>
          <div className="mb-6">
            <h2 className="text-2xl font-bold text-foreground">Live Data Pipeline</h2>
            <p className="text-muted-foreground">Real-time log ingestion and processing</p>
          </div>
          <LogStream />
        </section>

        {/* Delta Lake Analytics */}
        <section>
          <div className="mb-6">
            <h2 className="text-2xl font-bold text-foreground">Delta Lake Analytics</h2>
            <p className="text-muted-foreground">Interactive querying of your streaming data lake</p>
          </div>
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            <DeltaLakeExplorer onQuerySelect={(query) => {
              // This will be handled by the SqlQuery component
              const event = new CustomEvent('loadQuery', { detail: query });
              window.dispatchEvent(event);
            }} />
            <SqlQuery />
          </div>
        </section>
      </main>
    </div>
  );
};

export default Index;
