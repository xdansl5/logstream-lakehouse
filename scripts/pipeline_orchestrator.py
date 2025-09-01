#!/usr/bin/env python3
"""
Pipeline Orchestrator for LogStream Lakehouse
Manages the complete data pipeline including ML training, streaming, and monitoring
"""

import subprocess
import time
import argparse
import logging
import json
import os
from datetime import datetime
import requests
import signal
import sys

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PipelineOrchestrator:
    def __init__(self, config_file="pipeline_config.json"):
        self.script_dir = os.path.dirname(os.path.abspath(__file__))
        self.config = self.load_config(config_file)
        self.processes = {}
        self.running = False
        
        # Signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

    def load_config(self, config_file):
        """Load pipeline configuration"""
        default_config = {
            "kafka": {
                "servers": "localhost:9092",
                "topic": "web-logs"
            },
            "delta_lake": {
                "logs_path": "/tmp/delta-lake/rule-based-logs",
                "anomalies_path": "/tmp/delta-lake/anomalies",
                "ml_enriched_path": "/tmp/delta-lake/rule-based-logs",
                "ml_predictions_path": "/tmp/delta-lake/ml-predictions"
            },
            "checkpoints": {
                "logs": "/tmp/checkpoints/logs",
                "anomalies": "/tmp/checkpoints/anomalies",
                "ml_logs": "/tmp/checkpoints/ml-logs"
            },
            "elasticsearch": {
                "host": "localhost:9200",
                "enabled": True
            },
            "ml": {
                "training_samples": 10000,
                "retrain_interval_hours": 24
            },
            "monitoring": {
                "grafana_url": "http://localhost:3000",
                "kafka_ui_url": "http://localhost:8080",
                "kibana_url": "http://localhost:5601"
            }
        }
        
        config_path = config_file
        if not os.path.isabs(config_path):
            config_path = os.path.join(self.script_dir, config_file)

        if os.path.exists(config_path):
            try:
                with open(config_path, 'r') as f:
                    user_config = json.load(f)
                default_config.update(user_config)
                logger.info(f"✅ Loaded configuration from {config_path}")
            except Exception as e:
                logger.warning(f"⚠️ Error loading config file: {e}, using defaults")
        else:
            logger.info("📝 No config file found, using default configuration")
        
        return default_config

    def signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        logger.info(f"🛑 Received signal {signum}, shutting down gracefully...")
        self.shutdown()
        sys.exit(0)

    def check_services(self):
        """Check if required services are running"""
        logger.info("🔍 Checking service availability...")
        
        services_status = {}
        
        # Check Kafka
        try:
            response = requests.get(f"http://localhost:8080/api/clusters", timeout=5)
            services_status["kafka"] = response.status_code == 200
        except:
            services_status["kafka"] = False
        
        # Check Elasticsearch
        try:
            response = requests.get(f"{self.config['elasticsearch']['host']}/_cluster/health", timeout=5)
            services_status["elasticsearch"] = response.status_code == 200
        except:
            services_status["elasticsearch"] = False
        
        # Check Grafana
        try:
            response = requests.get(f"{self.config['monitoring']['grafana_url']}/api/health", timeout=5)
            services_status["grafana"] = response.status_code == 200
        except:
            services_status["grafana"] = False
        
        # Log status
        for service, status in services_status.items():
            status_icon = "✅" if status else "❌"
            logger.info(f"{status_icon} {service}: {'Running' if status else 'Not available'}")
        
        return services_status

    def setup_environment(self):
        """Setup the environment and directories"""
        logger.info("🔧 Setting up environment...")
        
        # Create directories
        directories = list({
            self.config["delta_lake"]["logs_path"],
            self.config["delta_lake"]["anomalies_path"],
            self.config["delta_lake"].get("ml_enriched_path", self.config["delta_lake"]["logs_path"]),
            self.config["delta_lake"].get("ml_predictions_path", "/tmp/delta-lake/ml-predictions"),
            self.config["checkpoints"]["logs"],
            self.config["checkpoints"]["anomalies"],
            self.config["checkpoints"]["ml_logs"]
        })
        
        for directory in directories:
            os.makedirs(directory, exist_ok=True)
            logger.info(f"📁 Created directory: {directory}")

    def train_ml_models(self):
        """Train ML models for anomaly detection"""
        logger.info("🔬 Training ML models...")
        
        try:
            # Generate training data first
            logger.info("📚 Generating training dataset...")
            subprocess.run([
                "python3", os.path.join(self.script_dir, "enhanced_log_generator.py"),
                "--training-data", str(self.config["ml"]["training_samples"]),
                "--output-file", "training_logs.json"
            ], check=True)
            
            # Train the model
            logger.info("🧠 Training anomaly detection model...")
            subprocess.run([
                "python3", os.path.join(self.script_dir, "ml_streaming_processor.py"),
                "--mode", "train",
                "--training-data", self.config["delta_lake"]["logs_path"]
            ], check=True)
            
            logger.info("✅ ML model training completed successfully")
            return True
            
        except subprocess.CalledProcessError as e:
            logger.error(f"❌ ML model training failed: {e}")
            return False

    def start_pipeline_components(self):
        """Start all pipeline components"""
        logger.info("🚀 Starting pipeline components...")
        
        # Start basic streaming processor
        logger.info("📊 Starting basic streaming processor...")
        self.processes["streaming_processor"] = subprocess.Popen([
            "python3", os.path.join(self.script_dir, "streaming_processor.py"),
            "--mode", "stream",
            "--kafka-servers", self.config["kafka"]["servers"],
            "--topic", self.config["kafka"]["topic"],
            "--output-path", self.config["delta_lake"]["logs_path"],
            "--checkpoint-path", self.config["checkpoints"]["logs"]
        ])
        
        # Wait a bit for the processor to start
        time.sleep(10)
        
        # Start ML streaming processor
        logger.info("🤖 Starting ML streaming processor...")
        elasticsearch_host = self.config["elasticsearch"]["host"] if self.config["elasticsearch"]["enabled"] else "none"
        self.processes["ml_processor"] = subprocess.Popen([
            "python3", os.path.join(self.script_dir, "ml_streaming_processor.py"),
            "--mode", "stream",
            "--kafka-servers", self.config["kafka"]["servers"],
            "--topic", self.config["kafka"]["topic"],
            "--output-path", self.config["delta_lake"]["logs_path"],
            "--ml-output-path", self.config["delta_lake"].get("ml_predictions_path", "/tmp/delta-lake/ml-predictions"),
            "--checkpoint-path", self.config["checkpoints"]["ml_logs"],
            "--elasticsearch-host", elasticsearch_host
        ])
        
        # Wait a bit for the ML processor to start
        time.sleep(10)
        
        # Start anomaly detector
        logger.info("🚨 Starting anomaly detector...")
        self.processes["anomaly_detector"] = subprocess.Popen([
            "python3", os.path.join(self.script_dir, "anomaly_detector.py"),
            "--mode", "detect",
            "--input-path", self.config["delta_lake"]["logs_path"],
            "--output-path", self.config["delta_lake"]["anomalies_path"],
            "--checkpoint-path", self.config["checkpoints"]["anomalies"]
        ])
        
        logger.info("✅ All pipeline components started")

    def start_log_generation(self):
        """Start log generation for testing"""
        logger.info("📝 Starting log generation...")
        
        self.processes["log_generator"] = subprocess.Popen([
            "python3", os.path.join(self.script_dir, "enhanced_log_generator.py"),
            "--kafka-servers", self.config["kafka"]["servers"],
            "--topic", self.config["kafka"]["topic"],
            "--rate", "20"
        ])
        
        logger.info("✅ Log generation started")

    def monitor_pipeline(self):
        """Monitor pipeline health and performance"""
        logger.info("📊 Starting pipeline monitoring...")
        
        try:
            while self.running:
                # Check process health
                for name, process in self.processes.items():
                    if process.poll() is not None:
                        logger.error(f"❌ Process {name} has stopped unexpectedly")
                        # Restart the process
                        self.restart_process(name)
                
                # Check service health every 30 seconds
                if int(time.time()) % 30 == 0:
                    self.check_services()
                
                time.sleep(5)
                
        except KeyboardInterrupt:
            logger.info("🛑 Monitoring interrupted")

    def restart_process(self, process_name):
        """Restart a failed process"""
        logger.info(f"🔄 Restarting {process_name}...")
        
        if process_name in self.processes:
            self.processes[process_name].terminate()
            time.sleep(5)
            
            # Restart based on process type
            if process_name == "streaming_processor":
                self.processes[process_name] = subprocess.Popen([
                    "python3", os.path.join(self.script_dir, "streaming_processor.py"),
                    "--mode", "stream",
                    "--kafka-servers", self.config["kafka"]["servers"],
                    "--topic", self.config["kafka"]["topic"],
                    "--output-path", self.config["delta_lake"]["logs_path"],
                    "--checkpoint-path", self.config["checkpoints"]["logs"]
                ])
            elif process_name == "ml_processor":
                elasticsearch_host = self.config["elasticsearch"]["host"] if self.config["elasticsearch"]["enabled"] else "none"
                self.processes[process_name] = subprocess.Popen([
                    "python3", os.path.join(self.script_dir, "ml_streaming_processor.py"),
                    "--mode", "stream",
                    "--kafka-servers", self.config["kafka"]["servers"],
                    "--topic", self.config["kafka"]["topic"],
                    "--output-path", self.config["delta_lake"]["logs_path"],
                    "--ml-output-path", self.config["delta_lake"].get("ml_predictions_path", "/tmp/delta-lake/ml-predictions"),
                    "--checkpoint-path", self.config["checkpoints"]["ml_logs"],
                    "--elasticsearch-host", elasticsearch_host
                ])
            elif process_name == "anomaly_detector":
                self.processes[process_name] = subprocess.Popen([
                    "python3", os.path.join(self.script_dir, "anomaly_detector.py"),
                    "--mode", "detect",
                    "--input-path", self.config["delta_lake"]["logs_path"],
                    "--output-path", self.config["delta_lake"]["anomalies_path"],
                    "--checkpoint-path", self.config["checkpoints"]["anomalies"]
                ])
            
            logger.info(f"✅ {process_name} restarted")

    def run_analytics(self):
        """Run analytics on the processed data"""
        logger.info("📈 Running analytics...")
        
        try:
            # Run basic analytics
            subprocess.run([
                "python3", os.path.join(self.script_dir, "streaming_processor.py"),
                "--mode", "analytics",
                "--output-path", self.config["delta_lake"]["logs_path"]
            ], check=True)
            
            # Run ML analytics
            subprocess.run([
                "python3", os.path.join(self.script_dir, "ml_streaming_processor.py"),
                "--mode", "analytics",
                "--output-path", self.config["delta_lake"].get("ml_predictions_path", "/tmp/delta-lake/ml-predictions")
            ], check=True)
            
            # Run anomaly analysis
            subprocess.run([
                "python3", os.path.join(self.script_dir, "anomaly_detector.py"),
                "--mode", "analyze",
                "--output-path", self.config["delta_lake"]["anomalies_path"]
            ], check=True)
            
            logger.info("✅ Analytics completed successfully")
            
        except subprocess.CalledProcessError as e:
            logger.error(f"❌ Analytics failed: {e}")

    def start_pipeline(self):
        """Start the complete pipeline"""
        logger.info("🚀 Starting LogStream Lakehouse Pipeline...")
        
        # Setup environment
        self.setup_environment()
        
        # Check services
        services_status = self.check_services()
        if not services_status.get("kafka", False):
            logger.error("❌ Kafka is not available. Please start the services first.")
            return False
        
        # Train ML models if needed
        if not os.path.exists("ml_models"):
            logger.info("🤖 No ML models found, starting training...")
            if not self.train_ml_models():
                logger.error("❌ ML model training failed")
                return False
        
        # Start pipeline components
        self.start_pipeline_components()
        
        # Start log generation for testing
        self.start_log_generation()
        
        # Set running flag
        self.running = True
        
        # Start monitoring
        self.monitor_pipeline()
        
        return True

    def shutdown(self):
        """Shutdown the pipeline gracefully"""
        logger.info("🛑 Shutting down pipeline...")
        
        self.running = False
        
        # Stop all processes
        for name, process in self.processes.items():
            logger.info(f"🛑 Stopping {name}...")
            try:
                process.terminate()
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                logger.warning(f"⚠️ Force killing {name}")
                process.kill()
        
        logger.info("✅ Pipeline shutdown completed")

    def show_status(self):
        """Show current pipeline status"""
        logger.info("📊 Pipeline Status:")
        
        # Check services
        services_status = self.check_services()
        
        # Check processes
        for name, process in self.processes.items():
            status = "🟢 Running" if process.poll() is None else "🔴 Stopped"
            logger.info(f"  {name}: {status}")
        
        # Show URLs
        logger.info("\n🌐 Access URLs:")
        logger.info(f"  Grafana: {self.config['monitoring']['grafana_url']}")
        logger.info(f"  Kafka UI: {self.config['monitoring']['kafka_ui_url']}")
        logger.info(f"  Kibana: {self.config['monitoring']['kibana_url']}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='LogStream Lakehouse Pipeline Orchestrator')
    parser.add_argument('--action', choices=['start', 'stop', 'status', 'analytics', 'train'], 
                       default='start', help='Action to perform')
    parser.add_argument('--config', default='pipeline_config.json', help='Configuration file')
    
    args = parser.parse_args()
    
    orchestrator = PipelineOrchestrator(args.config)
    
    try:
        if args.action == 'start':
            orchestrator.start_pipeline()
        elif args.action == 'stop':
            orchestrator.shutdown()
        elif args.action == 'status':
            orchestrator.show_status()
        elif args.action == 'analytics':
            orchestrator.run_analytics()
        elif args.action == 'train':
            orchestrator.train_ml_models()
    except KeyboardInterrupt:
        logger.info("🛑 Interrupted by user")
        orchestrator.shutdown()