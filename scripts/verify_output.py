#!/usr/bin/env python3
"""
Verify Output
Checks if output directories are being created and used correctly
"""

import os
import time
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def check_directories():
    """Check if required directories exist and have proper permissions"""
    logger.info("🔍 Checking directory structure...")
    
    directories = [
        "/tmp/delta-lake/logs",
        "/tmp/delta-lake/anomalies", 
        "/tmp/delta-lake/ml-enriched-logs",
        "/tmp/checkpoints/logs",
        "/tmp/checkpoints/anomalies",
        "/tmp/checkpoints/ml-logs"
    ]
    
    for directory in directories:
        if os.path.exists(directory):
            logger.info(f"✅ {directory} exists")
            
            # Check permissions
            try:
                # Try to create a test file
                test_file = os.path.join(directory, "test.txt")
                with open(test_file, 'w') as f:
                    f.write("test")
                os.remove(test_file)
                logger.info(f"✅ {directory} is writable")
            except Exception as e:
                logger.error(f"❌ {directory} is not writable: {e}")
        else:
            logger.warning(f"⚠️ {directory} does not exist")
            
            # Try to create it
            try:
                os.makedirs(directory, exist_ok=True)
                logger.info(f"✅ Created {directory}")
            except Exception as e:
                logger.error(f"❌ Failed to create {directory}: {e}")

def check_delta_lake_files():
    """Check if Delta Lake files are being created"""
    logger.info("🔍 Checking Delta Lake files...")
    
    delta_dirs = [
        "/tmp/delta-lake/logs",
        "/tmp/delta-lake/anomalies",
        "/tmp/delta-lake/ml-enriched-logs"
    ]
    
    for delta_dir in delta_dirs:
        if os.path.exists(delta_dir):
            # List contents
            try:
                contents = os.listdir(delta_dir)
                if contents:
                    logger.info(f"📁 {delta_dir} contains: {contents}")
                else:
                    logger.info(f"📁 {delta_dir} is empty")
            except Exception as e:
                logger.error(f"❌ Cannot read {delta_dir}: {e}")
        else:
            logger.warning(f"⚠️ {delta_dir} does not exist")

def check_checkpoint_files():
    """Check if checkpoint files are being created"""
    logger.info("🔍 Checking checkpoint files...")
    
    checkpoint_dirs = [
        "/tmp/checkpoints/logs",
        "/tmp/checkpoints/anomalies", 
        "/tmp/checkpoints/ml-logs"
    ]
    
    for checkpoint_dir in checkpoint_dirs:
        if os.path.exists(checkpoint_dir):
            try:
                contents = os.listdir(checkpoint_dir)
                if contents:
                    logger.info(f"📁 {checkpoint_dir} contains: {contents}")
                else:
                    logger.info(f"📁 {checkpoint_dir} is empty")
            except Exception as e:
                logger.error(f"❌ Cannot read {checkpoint_dir}: {e}")
        else:
            logger.warning(f"⚠️ {checkpoint_dir} does not exist")

def monitor_directory_changes(directory, duration=60):
    """Monitor a directory for changes over time"""
    logger.info(f"👀 Monitoring {directory} for changes over {duration} seconds...")
    
    if not os.path.exists(directory):
        logger.warning(f"⚠️ {directory} does not exist, cannot monitor")
        return
    
    initial_contents = set(os.listdir(directory))
    logger.info(f"📁 Initial contents: {initial_contents}")
    
    start_time = time.time()
    while time.time() - start_time < duration:
        current_contents = set(os.listdir(directory))
        
        if current_contents != initial_contents:
            new_files = current_contents - initial_contents
            removed_files = initial_contents - current_contents
            
            if new_files:
                logger.info(f"🆕 New files in {directory}: {new_files}")
            if removed_files:
                logger.info(f"🗑️ Removed files from {directory}: {removed_files}")
            
            initial_contents = current_contents
        
        time.sleep(5)
    
    logger.info(f"⏰ Monitoring completed for {directory}")

def main():
    """Main verification function"""
    logger.info("🚀 Starting output verification...")
    
    # Check directories
    check_directories()
    
    # Check existing files
    check_delta_lake_files()
    check_checkpoint_files()
    
    # Monitor for changes
    logger.info("\n" + "="*50)
    logger.info("MONITORING FOR CHANGES")
    logger.info("="*50)
    
    # Monitor the ml-enriched-logs directory specifically
    ml_dir = "/tmp/delta-lake/ml-enriched-logs"
    if os.path.exists(ml_dir):
        logger.info(f"👀 Monitoring {ml_dir} for changes...")
        monitor_directory_changes(ml_dir, duration=30)
    else:
        logger.warning(f"⚠️ {ml_dir} does not exist, cannot monitor")
    
    logger.info("\n" + "="*50)
    logger.info("VERIFICATION COMPLETED")
    logger.info("="*50)
    
    # Final status
    logger.info("📊 Final Status:")
    
    ml_dir_exists = os.path.exists("/tmp/delta-lake/ml-enriched-logs")
    ml_dir_writable = False
    
    if ml_dir_exists:
        try:
            test_file = "/tmp/delta-lake/ml-enriched-logs/test.txt"
            with open(test_file, 'w') as f:
                f.write("test")
            os.remove(test_file)
            ml_dir_writable = True
        except:
            pass
    
    logger.info(f"  ML enriched logs directory: {'✅ Exists' if ml_dir_exists else '❌ Missing'}")
    logger.info(f"  ML enriched logs writable: {'✅ Yes' if ml_dir_writable else '❌ No'}")
    
    if ml_dir_exists and ml_dir_writable:
        logger.info("✅ ML output directory is ready for use")
    else:
        logger.error("❌ ML output directory has issues")
        logger.info("💡 Try running: sudo mkdir -p /tmp/delta-lake/ml-enriched-logs && sudo chown -R $USER:$USER /tmp/delta-lake")

if __name__ == "__main__":
    main()