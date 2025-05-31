#!/usr/bin/env python3
"""
Startup script for continuous Clinical Trials ETL Pipeline

This script sets up and starts the scheduled ETL pipeline that runs every 10 minutes.
"""

import os
import sys
import signal
from datetime import datetime
from prefect_flows import create_scheduled_deployment

def setup_environment():
    """Set up environment variables for the ETL pipeline"""
    
    # Set default environment variables if not already set
    env_defaults = {
        'DB_HOST': 'postgres',
        'DB_PORT': '5432',
        'DB_NAME': 'clinical_trials_db',
        'DB_USER': 'postgres',
        'DB_PASSWORD': 'password',
        'DATA_FILE': '/app/data/clinical_trials.json',
        'DOWNLOAD_LIMIT': '1000',
        'DUPLICATE_ACTION': 'update',
        'SCHEDULE_INTERVAL_MINUTES': '10',
        'LOG_LEVEL': 'INFO',
        'PREFECT_API_URL': 'http://prefect-server:4200/api'
    }
    
    for key, default_value in env_defaults.items():
        if key not in os.environ:
            os.environ[key] = default_value
    
    print("🔧 Environment Configuration:")
    print(f"   Database: {os.environ['DB_HOST']}:{os.environ['DB_PORT']}/{os.environ['DB_NAME']}")
    print(f"   Download Limit: {os.environ['DOWNLOAD_LIMIT']} trials")
    print(f"   Schedule Interval: {os.environ['SCHEDULE_INTERVAL_MINUTES']} minutes")
    print(f"   Duplicate Action: {os.environ['DUPLICATE_ACTION']}")
    print(f"   Data File: {os.environ['DATA_FILE']}")
    print()

def signal_handler(sig, frame):
    """Handle Ctrl+C gracefully"""
    print('\n⏹️  Received interrupt signal. Stopping scheduled ETL pipeline...')
    sys.exit(0)

def main():
    """Main function to start the scheduled ETL pipeline"""
    
    # Set up signal handler for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    
    print("=" * 60)
    print("🚀 Clinical Trials ETL - Continuous Scheduler")
    print("=" * 60)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Setup environment
    setup_environment()
    
    # Start the scheduled deployment
    print("🕐 Starting scheduled ETL pipeline...")
    print("   The pipeline will run automatically every 10 minutes")
    print("   Monitor progress at: http://localhost:4200")
    print("   Press Ctrl+C to stop")
    print()
    
    try:
        create_scheduled_deployment()
    except KeyboardInterrupt:
        print("\n⏹️  Stopping scheduled ETL pipeline")
    except Exception as e:
        print(f"\n❌ Error starting scheduled deployment: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 