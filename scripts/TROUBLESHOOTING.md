# Troubleshooting Guide

## Common Issues and Solutions

### 1. PySpark ML Import Errors

#### Error: `ModuleNotFoundError: No module named 'distutils'`

**Problem**: This error occurs when using newer Python versions (3.12+) with PySpark, as `distutils` was deprecated and removed.

**Solutions**:

1. **Use Python 3.11 or earlier** (Recommended for production):
   ```bash
   # Check Python version
   python3 --version
   
   # If using Python 3.12+, consider downgrading to 3.11
   # or use a virtual environment with Python 3.11
   ```

2. **Use the simple streaming processor** (Fallback option):
   ```bash
   # Instead of ml_streaming_processor.py, use:
   python3 simple_streaming_processor.py --mode stream
   ```

3. **Install setuptools** (May help in some cases):
   ```bash
   pip install setuptools
   ```

4. **Use conda environment** with compatible Python version:
   ```bash
   conda create -n logstream python=3.11
   conda activate logstream
   pip install -r requirements.txt
   ```

#### Error: `ImportError: cannot import name 'VectorAssembler'`

**Problem**: PySpark ML libraries are not available or compatible.

**Solution**: The pipeline automatically falls back to rule-based anomaly detection when ML libraries are unavailable.

### 2. Spark Configuration Issues

#### Error: `java.lang.OutOfMemoryError: Java heap space`

**Problem**: Insufficient memory allocated to Spark.

**Solution**: Adjust Spark memory settings:
```bash
export SPARK_DRIVER_MEMORY=2g
export SPARK_EXECUTOR_MEMORY=2g
```

Or modify the processor scripts to include:
```python
.config("spark.driver.memory", "2g") \
.config("spark.executor.memory", "2g")
```

#### Error: `WARN NativeCodeLoader: Unable to load native-hadoop library`

**Problem**: This is a warning, not an error. Spark falls back to Java implementations.

**Solution**: This warning can be ignored. If you want to suppress it:
```python
.config("spark.sql.adaptive.enabled", "false")
```

### 3. Delta Lake Issues

#### Error: `java.lang.ClassNotFoundException: io.delta.sql.DeltaSparkSessionExtension`

**Problem**: Delta Lake JAR is not in the classpath.

**Solution**: Ensure Delta Lake is properly installed:
```bash
pip install delta-spark==3.0.0
```

### 4. Kafka Connection Issues

#### Error: `Failed to connect to broker`

**Problem**: Kafka is not running or not accessible.

**Solution**:
```bash
# Check if Kafka is running
docker-compose ps

# Start Kafka if needed
docker-compose up -d

# Wait for Kafka to be ready
sleep 30

# Check Kafka health
curl http://localhost:8080/api/clusters
```

### 5. Elasticsearch Issues

#### Error: `Connection refused` or `No route to host`

**Problem**: Elasticsearch is not running or not accessible.

**Solution**:
```bash
# Check Elasticsearch status
docker-compose ps elasticsearch

# Check Elasticsearch health
curl http://localhost:9200/_cluster/health

# Restart Elasticsearch if needed
docker-compose restart elasticsearch
```

### 6. Permission Issues

#### Error: `Permission denied` when writing to `/tmp`

**Problem**: Insufficient permissions for the `/tmp` directory.

**Solution**:
```bash
# Create directories with proper permissions
sudo mkdir -p /tmp/delta-lake/logs
sudo mkdir -p /tmp/delta-lake/anomalies
sudo mkdir -p /tmp/checkpoints/logs
sudo mkdir -p /tmp/checkpoints/anomalies

# Set ownership
sudo chown -R $USER:$USER /tmp/delta-lake
sudo chown -R $USER:$USER /tmp/checkpoints
```

### 7. Python Environment Issues

#### Error: `Module not found` for various packages

**Problem**: Python dependencies are not installed or in the wrong environment.

**Solution**:
```bash
# Install requirements
pip install -r requirements.txt

# Or install individually
pip install pyspark==3.5.0
pip install delta-spark==3.0.0
pip install kafka-python==2.0.2
pip install elasticsearch==8.11.0
```

### 8. Docker Issues

#### Error: `Cannot connect to the Docker daemon`

**Problem**: Docker is not running or user doesn't have permissions.

**Solution**:
```bash
# Start Docker
sudo systemctl start docker

# Add user to docker group (if needed)
sudo usermod -aG docker $USER
# Log out and back in, or run:
newgrp docker
```

## Testing and Validation

### 1. Test Basic Functionality

Run the basic functionality test to verify core components:
```bash
python3 test_basic_functionality.py
```

### 2. Test Individual Components

Test each component separately:
```bash
# Test basic streaming
python3 streaming_processor.py --mode stream

# Test simple streaming (no ML)
python3 simple_streaming_processor.py --mode stream

# Test anomaly detection
python3 anomaly_detector.py --mode detect
```

### 3. Check Service Health

Verify all services are running:
```bash
# Check Docker services
docker-compose ps

# Check service endpoints
curl http://localhost:8080/api/clusters  # Kafka
curl http://localhost:9200/_cluster/health  # Elasticsearch
curl http://localhost:3000/api/health  # Grafana
```

## Performance Tuning

### 1. Spark Configuration

For better performance, adjust these settings:
```python
.config("spark.sql.adaptive.enabled", "false") \
.config("spark.sql.streaming.statefulOperator.checkpointing.enabled", "true") \
.config("spark.sql.streaming.statefulOperator.checkpointing.interval", "10") \
.config("spark.memory.offHeap.enabled", "true") \
.config("spark.memory.offHeap.size", "1g")
```

### 2. Kafka Configuration

Adjust Kafka settings for higher throughput:
```yaml
# In docker-compose.yml
KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
KAFKA_MAX_MESSAGE_BYTES: 1048576
```

### 3. Elasticsearch Configuration

Optimize Elasticsearch for log ingestion:
```yaml
# In docker-compose.yml
ES_JAVA_OPTS: "-Xms1g -Xmx1g"
```

## Debug Mode

Enable debug logging for troubleshooting:
```python
import logging
logging.basicConfig(level=logging.DEBUG)

# In Spark
spark.sparkContext.setLogLevel("DEBUG")
```

## Getting Help

### 1. Check Logs

```bash
# Docker logs
docker-compose logs kafka
docker-compose logs elasticsearch
docker-compose logs grafana

# Application logs
# Check console output from the Python scripts
```

### 2. Common Commands

```bash
# Restart all services
docker-compose restart

# Stop all services
docker-compose down

# Start specific service
docker-compose up -d kafka

# Check resource usage
docker stats
```

### 3. Reset Environment

If all else fails, reset the environment:
```bash
# Stop all services
docker-compose down

# Remove volumes
docker-compose down -v

# Clean up temporary files
sudo rm -rf /tmp/delta-lake
sudo rm -rf /tmp/checkpoints

# Restart
docker-compose up -d
```

## Version Compatibility

### Supported Versions

- **Python**: 3.8, 3.9, 3.10, 3.11 (3.12+ may have compatibility issues)
- **PySpark**: 3.4.x, 3.5.x
- **Delta Lake**: 2.4.x, 3.0.x
- **Kafka**: 7.x
- **Elasticsearch**: 8.x
- **Grafana**: 10.x

### Known Issues

1. **Python 3.12+**: ML libraries compatibility issues
2. **PySpark 3.3 and below**: Delta Lake compatibility issues
3. **Elasticsearch 7.x**: Different API structure

## Quick Fixes

### Immediate Solutions

1. **ML Import Error**: Use `simple_streaming_processor.py`
2. **Memory Issues**: Reduce batch sizes and increase memory
3. **Connection Issues**: Check Docker services and wait for startup
4. **Permission Issues**: Fix directory permissions
5. **Version Conflicts**: Use compatible Python and package versions

### Fallback Options

- **No ML**: Use rule-based anomaly detection
- **No Elasticsearch**: Write only to Delta Lake
- **No Grafana**: Use console output for monitoring
- **No Kafka**: Use file-based input for testing