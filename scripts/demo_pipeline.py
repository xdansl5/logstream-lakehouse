#!/usr/bin/env python3
"""
Demo completo della pipeline Lakehouse
Esegue tutti i componenti in sequenza per dimostrare l'architettura completa
"""

import subprocess
import time
import threading
import signal
import sys
from pathlib import Path

class LakehousePipelineDemo:
    def __init__(self):
        self.processes = []
        self.running = True
        
    def signal_handler(self, sig, frame):
        """Gestisce l'interruzione graceful"""
        print('\n🛑 Interruzione ricevuta, fermando tutti i processi...')
        self.running = False
        self.stop_all_processes()
        sys.exit(0)
    
    def stop_all_processes(self):
        """Ferma tutti i processi avviati"""
        for process in self.processes:
            try:
                process.terminate()
                process.wait(timeout=10)
            except:
                process.kill()
    
    def run_command(self, command, name, delay=0):
        """Esegue un comando in un thread separato"""
        if delay > 0:
            time.sleep(delay)
            
        if not self.running:
            return
            
        print(f"🚀 Avviando {name}...")
        try:
            process = subprocess.Popen(command, shell=True)
            self.processes.append(process)
            process.wait()
        except Exception as e:
            print(f"❌ Errore in {name}: {e}")
    
    def start_demo(self):
        """Avvia la demo completa della pipeline"""
        print("🏗️ LAKEHOUSE ANALYTICS PLATFORM - DEMO COMPLETA")
        print("=" * 60)
        
        # Registra signal handler
        signal.signal(signal.SIGINT, self.signal_handler)
        
        # Verifica che i file esistano
        required_files = [
            'log_generator.py',
            'streaming_processor.py', 
            'anomaly_detector.py'
        ]
        
        for file in required_files:
            if not Path(file).exists():
                print(f"❌ File mancante: {file}")
                return
        
        print("✅ Tutti i file richiesti sono presenti")
        
        # Thread per i vari componenti
        threads = []
        
        # 1. Avvia il generatore di log (dopo 5 secondi)
        log_gen_thread = threading.Thread(
            target=self.run_command,
            args=["python3 log_generator.py --rate 8 --duration 300", "Log Generator", 5]
        )
        threads.append(log_gen_thread)
        
        # 2. Avvia il processore streaming (subito)
        streaming_thread = threading.Thread(
            target=self.run_command,
            args=["python3 streaming_processor.py --mode stream", "Streaming Processor", 0]
        )
        threads.append(streaming_thread)
        
        # 3. Avvia il rilevatore di anomalie (dopo 20 secondi)
        anomaly_thread = threading.Thread(
            target=self.run_command,
            args=["python3 anomaly_detector.py --mode detect", "Anomaly Detector", 20]
        )
        threads.append(anomaly_thread)
        
        # Avvia tutti i thread
        for thread in threads:
            thread.daemon = True
            thread.start()
        
        print("\n🎯 PIPELINE ATTIVA!")
        print("📊 Componenti in esecuzione:")
        print("   • Streaming Processor (Kafka → Delta Lake)")
        print("   • Log Generator (simulazione traffico web)")
        print("   • Anomaly Detector (rilevamento anomalie)")
        print("\n💡 Suggerimenti:")
        print("   • Controlla Kafka UI: http://localhost:8080")
        print("   • Esegui analytics: python3 streaming_processor.py --mode analytics")
        print("   • Premi Ctrl+C per fermare tutto")
        
        # Aspetta che tutti i thread terminino o interruzione
        try:
            while self.running and any(t.is_alive() for t in threads):
                time.sleep(1)
        except KeyboardInterrupt:
            self.signal_handler(signal.SIGINT, None)
        
        print("\n✅ Demo completata!")

    def run_analytics_demo(self):
        """Esegue demo delle funzionalità analytics"""
        print("📈 DEMO ANALYTICS")
        print("=" * 30)
        
        commands = [
            ("python3 streaming_processor.py --mode analytics", "Batch Analytics"),
            ("python3 anomaly_detector.py --mode analyze", "Anomaly Analysis"),
            ("python3 streaming_processor.py --mode optimize", "Delta Lake Optimization")
        ]
        
        for command, name in commands:
            print(f"\n🔍 Eseguendo {name}...")
            try:
                result = subprocess.run(command, shell=True, capture_output=True, text=True)
                if result.returncode == 0:
                    print(f"✅ {name} completato")
                    if result.stdout:
                        print(result.stdout)
                else:
                    print(f"❌ Errore in {name}: {result.stderr}")
            except Exception as e:
                print(f"❌ Errore nell'esecuzione di {name}: {e}")
        
        print("\n✅ Demo analytics completata!")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Demo completo della pipeline Lakehouse')
    parser.add_argument('--mode', choices=['full', 'analytics'], 
                       default='full', help='Modalità demo')
    
    args = parser.parse_args()
    
    demo = LakehousePipelineDemo()
    
    if args.mode == 'full':
        demo.start_demo()
    elif args.mode == 'analytics':
        demo.run_analytics_demo()