# LogStream Lakehouse - ML-Powered Real-Time Log Processing Pipeline

## Overview

This is a comprehensive, production-ready real-time log processing pipeline that integrates modern data engineering technologies for 2025 workflows. The pipeline processes web application logs in real-time, applies machine learning models for anomaly detection, and provides interactive dashboards for monitoring.

## 🏗️ Architecture

```
Web Applications → Kafka → Spark Structured Streaming → ML Models → Delta Lake + Elasticsearch
                                    ↓
                              Grafana Dashboards
```

### Components

1. **Data Ingestion**: Kafka for high-throughput log streaming
2. **Real-time Processing**: Spark Structured Streaming with ML inference
3. **Storage**: Delta Lake for ACID transactions + Elasticsearch for search
4. **ML Pipeline**: Real-time anomaly detection using K-means clustering
5. **Monitoring**: Grafana dashboards with real-time metrics
6. **Orchestration**: Automated pipeline management and health monitoring

## 🚀 Quick Start

### 1. Start Infrastructure Services

```bash
# Start all services (Kafka, Elasticsearch, Grafana, etc.)
docker-compose up -d

# Wait for services to be ready (about 2-3 minutes)
docker-compose ps
```

### 2. Setup Environment

```bash
# Install Python dependencies
pip install -r requirements.txt

# Setup directories and environment
./setup_environment.sh
```

### 3. Start the Pipeline

```bash
# Start the complete pipeline
python3 pipeline_orchestrator.py --action start

# Or start components individually:
python3 enhanced_log_generator.py --rate 20
python3 ml_streaming_processor.py --mode stream
python3 anomaly_detector.py --mode detect
```

## 📊 Access Points

- **Grafana Dashboard**: http://localhost:3000 (admin/admin)
- **Kafka UI**: http://localhost:8080
- **Kibana**: http://localhost:5601
- **MLflow**: http://localhost:5000

## 🔧 Configuration

### Pipeline Configuration

Create `pipeline_config.json` to customize settings:

```json
{
  "kafka": {
    "servers": "localhost:9092",
    "topic": "web-logs"
  },
  "elasticsearch": {
    "host": "localhost:9200",
    "enabled": true
  },
  "ml": {
    "training_samples": 10000,
    "retrain_interval_hours": 24
  }
}
```

### Environment Variables

```bash
export SPARK_HOME=/opt/spark
export PYSPARK_PYTHON=python3
```

## 🤖 Machine Learning Features

### Anomaly Detection

- **K-means Clustering**: Identifies unusual patterns in log data
- **Real-time Scoring**: Provides anomaly scores for each log entry
- **Feature Engineering**: Automatic extraction of ML features from logs

### ML Pipeline

1. **Training**: Uses historical log data to train models
2. **Inference**: Real-time prediction on streaming data
3. **Model Management**: MLflow integration for model versioning

### Features Used

- Response time patterns
- Status code distributions
- Request size analysis
- IP address patterns
- Temporal patterns (hour, day of week)

## 📈 Monitoring & Analytics

### Real-time Metrics

- Log volume and throughput
- Anomaly detection rates
- Response time distributions
- Error rates by endpoint
- ML model confidence levels

### Dashboards

- **Operational Dashboard**: Real-time pipeline health
- **Anomaly Dashboard**: Detected anomalies and patterns
- **Performance Dashboard**: Response times and throughput
- **ML Dashboard**: Model performance and predictions

## 🛠️ Development & Testing

### Generate Test Data

```bash
# Generate logs for testing
python3 enhanced_log_generator.py --rate 50 --duration 300

# Generate training dataset
python3 enhanced_log_generator.py --training-data 10000
```

### Run Analytics

```bash
# Run ML analytics
python3 ml_streaming_processor.py --mode analytics

# Run anomaly analysis
python3 anomaly_detector.py --mode analyze
```

### Pipeline Management

```bash
# Check pipeline status
python3 pipeline_orchestrator.py --action status

# Run analytics
python3 pipeline_orchestrator.py --action analytics

# Train ML models
python3 pipeline_orchestrator.py --action train
```

## 🔍 Troubleshooting

### Common Issues

1. **Anomaly Detector Error**: Fixed `countDistinct` issue by using `approx_count_distinct`
2. **Kafka Connection**: Ensure Docker services are running
3. **Memory Issues**: Adjust Spark memory settings in ML processor
4. **Elasticsearch**: Check if single-node discovery is working

### Debug Commands

```bash
# Check service health
docker-compose ps

# View logs
docker-compose logs kafka
docker-compose logs elasticsearch

# Check pipeline status
python3 pipeline_orchestrator.py --action status
```

## 📚 API Reference

### ML Streaming Processor

```python
from ml_streaming_processor import MLLogProcessor

processor = MLLogProcessor()
processor.start_ml_streaming(
    kafka_servers="localhost:9092",
    topic="web-logs",
    elasticsearch_host="localhost:9200"
)
```

### Anomaly Detector

```python
from anomaly_detector import AnomalyDetector

detector = AnomalyDetector()
detector.start_anomaly_detection(
    delta_path="/tmp/delta-lake/logs",
    output_path="/tmp/delta-lake/anomalies"
)
```

## 🚀 Production Deployment

### Scaling Considerations

- **Kafka**: Increase partitions and replicas
- **Spark**: Adjust executor memory and cores
- **Elasticsearch**: Add nodes for clustering
- **Monitoring**: Use Prometheus + Grafana for production

### Security

- Enable authentication for Elasticsearch
- Use SSL/TLS for Kafka
- Implement proper access controls
- Monitor for suspicious activities

## 📊 Performance Metrics

### Expected Performance

- **Throughput**: 10,000+ logs/second
- **Latency**: < 100ms end-to-end
- **ML Inference**: < 50ms per log entry
- **Storage**: Efficient Delta Lake compression

### Optimization Tips

1. **Partitioning**: Use date/hour partitioning for Delta Lake
2. **Caching**: Leverage Spark caching for repeated queries
3. **Checkpointing**: Regular checkpointing for fault tolerance
4. **Resource Tuning**: Optimize Spark executor configuration

## 🤝 Contributing

### Development Setup

1. Fork the repository
2. Create feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit pull request

### Testing

```bash
# Run unit tests
python -m pytest tests/

# Run integration tests
python -m pytest tests/integration/
```

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙏 Acknowledgments

- Apache Spark community
- Delta Lake project
- Elasticsearch team
- Grafana community
- MLflow contributors

---

For more information, see the main [README.md](../README.md) or contact the development team.