# Real-Time Data Pipeline Platform

An interactive platform for ingesting, processing, and analyzing real-time web logs using a modern data stack. The project features a complete end-to-end pipeline with Apache Kafka, Spark Structured Streaming, and Delta Lake, a user-friendly web interface, and optional machine learning for anomaly detection.

This repository contains all the necessary scripts and configurations to run the pipeline, the backend API, and the frontend application.

-----

## 🚀 Key Features

### Live Metrics & Monitoring

  * **Real-Time Data**: Get a live stream of logs and metrics directly from the pipeline.
  * **Dynamic UI**: The web platform's charts and dashboards automatically update with new incoming data.
  * **Pipeline Health**: Visual indicators show the real-time health and connection status of each pipeline component.
  * **Anomaly Detection**: Automatically identify and flag unusual log patterns or errors.

### End-to-End Pipeline

  * **No Mock Data**: All data displayed and analyzed is processed directly from the live pipeline.
  * **Kafka & Spark Integration**: A direct, streaming connection to Kafka topics is used to process data with Spark Structured Streaming.
  * **Query Delta Lake**: Execute custom Spark SQL queries against the stored data in Delta Lake tables.

### Interactive Interface

  * **Dashboard**: A main dashboard with a metrics grid, live charts, and a log stream.
  * **Query Builder**: A UI to build and run custom Spark SQL queries.
  * **Advanced Filtering**: Filter logs and metrics by level, source, or endpoint.

-----

## 🏗️ Architecture

The platform's architecture follows a standard data pipeline pattern, ensuring data flows seamlessly from source to analysis.

```
       Web Logs (Sources)
              |
              v
     Apache Kafka (Broker)
              |
              v
    Spark Streaming (Processing)
              |
              v
      Delta Lake (Storage)
              |
              v
     API Server (Node.js)
              |
              v
    Web Platform (React App)
```

### Singleton Spark Session

To maintain stability and prevent common errors like "Only one SparkContext may be running," the platform uses a singleton pattern for managing the Spark session. This ensures that all components, including the streaming processor and ML detector, share a single, consistent Spark instance, leading to efficient resource use and consistent configuration.

The shared session logic is implemented in `spark_session_manager.py`.

-----

## 📦 Technologies

### Frontend

  * **React 18** with TypeScript
  * **Vite** for a fast build and development experience
  * **shadcn/ui** & **Tailwind CSS** for a modern, component-based UI
  * **Recharts** for interactive data visualizations
  * **React Query** for efficient state management and data fetching

### Backend

  * **Node.js** with **Express**
  * **KafkaJS** for robust Kafka connectivity
  * **Server-Sent Events (SSE)** for real-time data streaming to the frontend
  * **REST API** for serving metrics and query results

### Data Pipeline

  * **Apache Kafka**: A distributed event streaming platform for ingesting web logs.
  * **Apache Spark**: A unified analytics engine for processing streaming data.
  * **Delta Lake**: An open-source storage layer that brings ACID transactions and reliability to data lakes.
  * **Spark SQL**: Used for performing powerful analytics and queries on the data.

-----

## 🛠️ Installation & Setup

### Prerequisites

  * [Node.js](https://nodejs.org/) 18+ and npm
  * [Docker & Docker Compose](https://docs.docker.com/compose/install/) (recommended for a quick setup)
  * [Apache Spark](https://spark.apache.org/downloads.html) (locally or in a cluster)

### 1\. Clone the repository

```bash
git clone https://github.com/YOUR-USERNAME/YOUR-REPOSITORY.git
cd data-pipeline-platform
```

### 2\. Configure environment variables

```bash
cp .env.example .env
```

Edit the `.env` file with your specific configuration, such as API URLs and Kafka brokers.

### 3\. Install dependencies

```bash
npm install
```

-----

## 🚀 Running the Platform

We recommend using the provided orchestration script for a seamless start.

### Quick Start (Recommended)

This method automatically sets up all the required services, including Kafka and the data generator.

1.  Navigate to the `scripts` directory.
2.  Install Python dependencies:
    ```bash
    pip install -r requirements.txt
    ```
3.  Run the orchestration script:
    ```bash
    python3 pipeline_orchestrator.py --action start
    ```

This command will:

  * Start Kafka and a test log generator.
  * Set up the necessary directories.
  * Launch all Spark-based processors.

To stop the entire pipeline, run:

```bash
python3 pipeline_orchestrator.py --action stop
```

### Manual Setup

If you prefer to run each component individually, you can use the following commands from the `scripts/` directory.

#### Start Data Generation

```bash
python3 enhanced_log_generator.py --rate 10
```

#### Start Spark Processors

```bash
# Start the main streaming processor
python3 streaming_processor.py --mode stream

# Start the ML anomaly detector
python3 anomaly_detector.py --mode detect
```

#### Start Frontend & Backend

In the root directory of the project:

```bash
# Start the backend server
npm run server

# Start the frontend app
npm run dev
```

The frontend will be available at `http://localhost:5173`.

-----

## 📊 Analytics & Maintenance

The pipeline provides various modes for running analytics jobs and performing maintenance.

### Batch Analytics

  * **Rule-based logs**: `python3 streaming_processor.py --mode analytics`
  * **Anomaly data**: `python3 anomaly_detector.py --mode analyze`

### Delta Lake Optimization

To optimize the stored data for faster queries:

```bash
python3 streaming_processor.py --mode optimize
```

-----

## ❓ Troubleshooting

### Connectivity Issues

  * **Kafka**: Ensure it's running on `localhost:9092`. You can use `docker-compose up` in the `scripts/` directory.
  * **Spark**: Check if the Spark master is available on `localhost:7077`.
  * **Delta Lake**: Verify the output path (`/tmp/delta-lake` by default) has the correct read/write permissions.

### Debugging

  * **Backend logs**: Check the console output of `npm run server`.
  * **API checks**: Use `curl` to verify the backend is running and responding:
    ```bash
    curl http://localhost:4000/api/metrics
    ```

-----

## 🤝 Contributing

Contributions are welcome\! Please follow these steps:

1.  Fork the repository.
2.  Create a new branch (`git checkout -b feature/your-feature-name`).
3.  Make your changes and commit them (`git commit -m 'Add your feature'`).
4.  Push to the branch (`git push origin feature/your-feature-name`).
5.  Open a Pull Request with a clear description of your changes.

-----

## 📄 License

This project is licensed under the MIT License. See the [LICENSE](https://www.google.com/search?q=LICENSE) file for details.
