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

For a smooth start, we recommend using Docker containers and the orchestration script.

### Running with Docker (Recommended)

Using Docker Compose is the easiest way to start the entire pipeline infrastructure (Kafka, Zookeeper, and Kafka UI).

1.  Make sure you have Docker and Docker Compose installed and running.

2.  From your terminal, in the root directory of the project, start the services:

    ```bash
    docker-compose up -d
    ```

    This command will run the containers in the background. It may take a few minutes.

3.  Verify that the containers are running:

    ```bash
    docker-compose ps
    ```

Once the containers are active, the infrastructure is ready:

  * **Kafka UI**: `http://localhost:8080`
  * **Kafka**: `localhost:9092`

### Running the Pipeline and Web App

After starting the infrastructure with Docker, you can run the rest of the pipeline and the user interface.

1.  In the `scripts/` directory, install the Python dependencies:

    ```bash
    pip install -r requirements.txt
    ```

2.  Run the orchestration script to start the Spark processors and the log generator:

    ```bash
    python3 pipeline_orchestrator.py --action start
    ```

    **Note:** Since Kafka is already running in a container, the script will only start the Spark components and the log generator.

3.  In another terminal, in the root directory of the project, start the backend and frontend:

    ```bash
    # Start the backend server
    npm run server

    # Start the frontend app
    npm run dev
    ```

    The frontend application will be available at `http://localhost:5173`.

### Stopping the Services

To stop the entire platform cleanly:

1.  **Stop the Spark processors and log generator**:

    ```bash
    python3 scripts/pipeline_orchestrator.py --action stop
    ```

2.  **Stop the Docker containers**:

    ```bash
    docker-compose down
    ```

3.  **Stop the Node.js processes**: Manually interrupt the terminals where `npm run server` and `npm run dev` are running (e.g., with `Ctrl + C`).

-----

## 📊 Analytics & Maintenance

The pipeline provides various modes for running analytics and maintenance jobs.

### Batch Analytics

  * **Rule-based logs**: `python3 streaming_processor.py --mode analytics`
  * **Anomaly data**: `python3 anomaly_detector.py --mode analyze`

### Delta Lake Optimization

To optimize the stored data and improve query performance:

```bash
python3 streaming_processor.py --mode optimize
```

-----

## ❓ Troubleshooting

### Connectivity Issues

  * **Kafka**: Ensure the `kafka` container is running and its ports are mapped correctly.
  * **Spark**: If Spark is running in a container, verify that your Python script can connect to its master.
  * **Paths**: If you're using containers for Spark, make sure the paths (e.g., `/tmp/delta-lake`) are mapped as volumes between the host and the container.

### Debugging

  * **Container logs**: Use `docker-compose logs <service_name>` to inspect the logs of a specific service (e.g., `kafka`).
  * **Backend logs**: Check the console output of `npm run server`.
  * **API checks**: Use `curl` to verify that the backend is active and responding:
    ```bash
    curl http://localhost:4000/api/metrics
    ```

-----

## 🤝 Contributing

Contributions are welcome\! Please follow these steps:

1.  Fork the repository.
2.  Create a new branch (`git checkout -b feature/your-feature-name`).
3.  Make your changes and commit them (`git commit -m 'Add your awesome feature'`).
4.  Push to the branch (`git push origin feature/your-feature-name`).
5.  Open a Pull Request with a clear description of your changes.

-----

## 📄 License

This project is licensed under the MIT License. See the [LICENSE](https://www.google.com/search?q=LICENSE) file for details.