#!/usr/bin/env python3
"""
Quick start script per la Lakehouse Platform
Avvia tutti i componenti necessari per la demo
"""

import subprocess
import sys
import os
import time
import signal
from pathlib import Path

class LakehouseStarter:
    def __init__(self):
        self.processes = []
        self.script_dir = Path(__file__).parent

    def check_requirements(self):
        """Verifica che i requisiti siano installati"""
        try:
            import kafka
            import pyspark
            import delta
            import fastapi
            print("✅ Tutti i requisiti Python sono installati")
            return True
        except ImportError as e:
            print(f"❌ Requisito mancante: {e}")
            print("Installa i requisiti con: pip install -r requirements.txt")
            return False

    def start_web_api(self):
        """Avvia il server Web API"""
        print("🚀 Avvio Web API Server...")
        cmd = [sys.executable, str(self.script_dir / "web_api_server.py")]
        process = subprocess.Popen(cmd)
        self.processes.append(("Web API Server", process))
        return process

    def start_log_generator(self, rate=10):
        """Avvia il generatore di log"""
        print(f"📝 Avvio Log Generator (rate: {rate} logs/sec)...")
        cmd = [
            sys.executable, str(self.script_dir / "log_generator.py"),
            "--rate", str(rate),
            "--kafka-servers", "localhost:9092"
        ]
        process = subprocess.Popen(cmd)
        self.processes.append(("Log Generator", process))
        return process

    def start_streaming_processor(self):
        """Avvia Spark Streaming Processor"""
        print("⚡ Avvio Spark Streaming Processor...")
        cmd = [
            sys.executable, str(self.script_dir / "streaming_processor.py"),
            "--mode", "stream"
        ]
        process = subprocess.Popen(cmd)
        self.processes.append(("Streaming Processor", process))
        return process

    def start_anomaly_detector(self):
        """Avvia Anomaly Detector"""
        print("🔍 Avvio Anomaly Detector...")
        time.sleep(5)  # Wait for some data to be processed
        cmd = [
            sys.executable, str(self.script_dir / "anomaly_detector.py"),
            "--mode", "detect"
        ]
        process = subprocess.Popen(cmd)
        self.processes.append(("Anomaly Detector", process))
        return process

    def signal_handler(self, sig, frame):
        """Gestisce l'interruzione graceful"""
        print("\n🛑 Fermando tutti i processi...")
        for name, process in self.processes:
            try:
                process.terminate()
                process.wait(timeout=5)
                print(f"✅ {name} fermato")
            except subprocess.TimeoutExpired:
                process.kill()
                print(f"🔪 {name} terminato forzatamente")
        sys.exit(0)

    def start_full_demo(self):
        """Avvia la demo completa"""
        if not self.check_requirements():
            return False

        # Setup signal handler
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

        print("🏗️  Avvio Lakehouse Analytics Platform...")
        print("=" * 50)

        try:
            # 1. Start Web API Server first
            api_process = self.start_web_api()
            time.sleep(3)

            # 2. Start Log Generator
            log_gen_process = self.start_log_generator(rate=15)
            time.sleep(2)

            # 3. Start Streaming Processor  
            streaming_process = self.start_streaming_processor()
            time.sleep(5)

            # 4. Start Anomaly Detector
            anomaly_process = self.start_anomaly_detector()

            print("\n" + "=" * 50)
            print("🎉 Lakehouse Platform avviata con successo!")
            print("\n📊 Accessi:")
            print("   • Dashboard:     http://localhost:3000")
            print("   • API Backend:   http://localhost:8000")
            print("   • WebSocket:     ws://localhost:8000/ws")
            print("   • API Docs:      http://localhost:8000/docs")
            print("\n🛠️  Processi attivi:")
            for name, _ in self.processes:
                print(f"   • {name}")
            
            print("\n💡 Per fermare tutti i processi: Ctrl+C")
            print("=" * 50)

            # Wait for processes
            while True:
                # Check if any process died
                for name, process in self.processes:
                    if process.poll() is not None:
                        print(f"⚠️  {name} è terminato inaspettatamente")
                        return False
                
                time.sleep(1)

        except Exception as e:
            print(f"❌ Errore durante l'avvio: {e}")
            self.signal_handler(None, None)
            return False

    def quick_demo(self):
        """Avvia solo i componenti essenziali per la demo"""
        print("🚀 Avvio Demo Rapida (solo simulazione)...")
        
        # Start only Web API Server
        api_process = self.start_web_api()
        
        print("\n" + "=" * 50)
        print("✅ Demo rapida avviata!")
        print("📊 Dashboard: http://localhost:3000")
        print("🔧 API: http://localhost:8000")
        print("💡 Modalità: Solo simulazione (niente Kafka/Spark reali)")
        print("=" * 50)
        
        try:
            api_process.wait()
        except KeyboardInterrupt:
            self.signal_handler(None, None)

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Lakehouse Platform Starter")
    parser.add_argument('--mode', choices=['full', 'quick'], default='quick',
                       help='Modalità di avvio (full: con Kafka/Spark, quick: solo API)')
    parser.add_argument('--log-rate', type=int, default=15,
                       help='Rate di generazione log (logs/sec)')
    
    args = parser.parse_args()
    
    starter = LakehouseStarter()
    
    if args.mode == 'full':
        starter.start_full_demo()
    else:
        starter.quick_demo()