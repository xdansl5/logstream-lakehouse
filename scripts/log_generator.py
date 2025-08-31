#!/usr/bin/env python3
"""
Log Generator - Simulates web server logs and sends them to Kafka
"""

import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
import argparse


# -----------------------------------------------------------------------------
# CLASS: LogGenerator
# - Initializes a Kafka producer with JSON serialization
# - Stores a topic name where logs will be sent
# - Defines sample data (user agents, endpoints, methods, status codes)
#   to generate realistic web server logs
# -----------------------------------------------------------------------------
class LogGenerator:
    def __init__(self, kafka_bootstrap_servers='localhost:9092', topic='web-logs'):
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.topic = topic
        
        # Sample data for realistic logs
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36'
        ]
        
        self.endpoints = [
            '/api/users', '/api/orders', '/api/products', '/api/auth/login',
            '/api/auth/logout', '/api/search', '/api/analytics', '/dashboard',
            '/profile', '/checkout', '/api/recommendations', '/api/cart'
        ]
        
        self.methods = ['GET', 'POST', 'PUT', 'DELETE']
        self.status_codes = [200, 201, 400, 401, 404, 500, 502, 503]
        self.status_weights = [0.7, 0.1, 0.05, 0.03, 0.05, 0.02, 0.02, 0.03]


    # -----------------------------------------------------------------------------
    # METHOD: generate_log_entry
    # - Generates one synthetic log entry with:
    #   * timestamp, IP, HTTP method, endpoint
    #   * status code (weighted probability)
    #   * response time (depends on status)
    #   * user_agent, user_id, session_id
    #   * bytes_sent, referer
    # - Returns the log entry as a dictionary
    # -----------------------------------------------------------------------------
    def generate_log_entry(self):
        timestamp = datetime.now().isoformat()
        ip = f"192.168.{random.randint(1,255)}.{random.randint(1,255)}"
        method = random.choice(self.methods)
        endpoint = random.choice(self.endpoints)
        status_code = random.choices(self.status_codes, weights=self.status_weights)[0]
        
        # Response time depends on status code
        if status_code >= 500:
            response_time = random.randint(2000, 5000)  # Server errors are slow
        elif status_code >= 400:
            response_time = random.randint(100, 800)    # Client errors
        else:
            response_time = random.randint(50, 500)     # Success responses
        
        log_entry = {
            'timestamp': timestamp,
            'ip': ip,
            'method': method,
            'endpoint': endpoint,
            'status_code': status_code,
            'response_time': response_time,
            'user_agent': random.choice(self.user_agents),
            'user_id': f"user_{random.randint(1000, 9999)}" if random.random() > 0.3 else None,
            'session_id': f"session_{random.randint(100000, 999999)}",
            'bytes_sent': random.randint(200, 50000),
            'referer': f"https://example.com{random.choice(self.endpoints)}" if random.random() > 0.5 else None
        }
        
        return log_entry


    # -----------------------------------------------------------------------------
    # METHOD: start_streaming
    # - Starts a loop to continuously generate logs
    # - Sends each generated log entry to Kafka
    # - Respects a configured rate of logs per second
    # - Stops after a specified duration or when interrupted
    # - Prints progress every 100 logs
    # -----------------------------------------------------------------------------
    def start_streaming(self, rate_per_second=5, duration_seconds=None):
        print(f"Starting log generation at {rate_per_second} logs/second")
        print(f"Sending to Kafka topic: {self.topic}")
        
        count = 0
        start_time = time.time()
        
        try:
            while True:
                if duration_seconds and (time.time() - start_time) > duration_seconds:
                    break
                
                log_entry = self.generate_log_entry()
                
                # Send to Kafka
                self.producer.send(self.topic, log_entry)
                count += 1
                
                if count % 100 == 0:
                    print(f"Sent {count} log entries...")
                
                # Enforce log generation rate
                sleep_time = 1.0 / rate_per_second
                time.sleep(sleep_time)
                
        except KeyboardInterrupt:
            print(f"\nStopping log generation. Sent {count} total entries.")
        finally:
            self.producer.close()


# -----------------------------------------------------------------------------
# MAIN EXECUTION
# - Parses command-line arguments for rate, duration, Kafka servers, and topic
# - Instantiates a LogGenerator with the given parameters
# - Starts the streaming process
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate web server logs and send to Kafka')
    parser.add_argument('--rate', type=int, default=5, help='Logs per second (default: 5)')
    parser.add_argument('--duration', type=int, help='Duration in seconds (default: infinite)')
    parser.add_argument('--kafka-servers', default='localhost:9092', help='Kafka bootstrap servers')
    parser.add_argument('--topic', default='web-logs', help='Kafka topic name')
    
    args = parser.parse_args()
    
    generator = LogGenerator(
        kafka_bootstrap_servers=args.kafka_servers,
        topic=args.topic
    )
    
    generator.start_streaming(
        rate_per_second=args.rate,
        duration_seconds=args.duration
    )
