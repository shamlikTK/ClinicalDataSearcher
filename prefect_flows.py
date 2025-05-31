#!/usr/bin/env python3
"""
Clinical Trials ETL Prefect Flows

Defines Prefect workflows for clinical trials data download and ETL processing.
Provides web UI for monitoring and management with scheduling capabilities.
"""

import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, Optional

from prefect import flow, task, get_run_logger
from dotenv import load_dotenv

# Import our ETL and download classes
from download_clinical_data import ClinicalTrialsDownloader
from etl_pipeline import ClinicalTrialsETL

# Load environment variables
load_dotenv()

@task(retries=3, retry_delay_seconds=1800)  # 30 minutes retry delay
def download_clinical_data(limit: Optional[int] = None) -> Dict[str, Any]:
    """
    Download clinical trials data from ClinicalTrials.gov API
    
    Args:
        limit: Maximum number of trials to download. If None, uses environment variable.
    
    Returns:
        Dict containing execution results and metadata
    """
    logger = get_run_logger()
    logger.info("Starting clinical trials data download")
    
    try:
        # Initialize the downloader from environment
        downloader = ClinicalTrialsDownloader.from_environment()
        
        # Use provided limit or get from environment
        if limit is None:
            limit = int(os.getenv('DOWNLOAD_LIMIT', '10000'))
        
        logger.info(f"Download limit: {limit} trials")
        logger.info(f"Target file: {downloader.data_file}")
        
        # Download and save data
        results = downloader.download_and_save(limit=limit)
        
        logger.info(f"Download completed successfully")
        logger.info(f"File size: {results.get('file_size_mb', 0):.2f} MB")
        logger.info(f"Trials downloaded: {results.get('trial_count', 0)}")
        
        return results
        
    except Exception as e:
        logger.error(f"Download failed: {e}")
        raise

@task(retries=2, retry_delay_seconds=900)  # 15 minutes retry delay
def run_etl_pipeline(download_result: Optional[Dict[str, Any]] = None, data_file: Optional[str] = None) -> Dict[str, Any]:
    """
    Run the ETL pipeline to process downloaded data
    
    Args:
        download_result: Result from the download task (optional)
        data_file: Path to data file (optional, uses environment if not provided)
        
    Returns:
        Dict containing execution results and metadata
    """
    logger = get_run_logger()
    logger.info("Starting ETL pipeline processing")
    
    if download_result:
        logger.info(f"Download status: {download_result['status']}")
        logger.info(f"Processing file: {download_result.get('data_file', 'N/A')}")
        # Use data file from download result if provided
        if not data_file and 'data_file' in download_result:
            data_file = download_result['data_file']
    
    try:
        # Initialize ETL pipeline from environment
        etl = ClinicalTrialsETL.from_environment()
        
        logger.info(f"Database host: {etl.db_config.host}")
        logger.info(f"Database name: {etl.db_config.database}")
        logger.info(f"Duplicate action: {etl.duplicate_action}")
        
        # Run the complete ETL process
        results = etl.run_etl(data_file=data_file)
        
        logger.info(f"ETL processing completed successfully")
        logger.info(f"Total trials: {results.get('total_trials', 0)}")
        logger.info(f"Processed: {results.get('processed_count', 0)}")
        logger.info(f"Skipped: {results.get('skipped_count', 0)}")
        logger.info(f"Updated: {results.get('updated_count', 0)}")
        logger.info(f"Errors: {results.get('error_count', 0)}")
        
        return results
        
    except Exception as e:
        logger.error(f"ETL pipeline failed: {e}")
        raise

@task
def send_notification(download_result: Dict[str, Any], etl_result: Dict[str, Any]) -> None:
    """
    Send notification about pipeline completion
    
    Args:
        download_result: Result from download task
        etl_result: Result from ETL task
    """
    logger = get_run_logger()
    
    logger.info("=== CLINICAL TRIALS ETL PIPELINE SUMMARY ===")
    logger.info(f"Download Status: {download_result['status']}")
    logger.info(f"Download Duration: {download_result['duration']}")
    if download_result.get('file_size_mb'):
        logger.info(f"Downloaded File Size: {download_result['file_size_mb']:.2f} MB")
    if download_result.get('trial_count'):
        logger.info(f"Trials Downloaded: {download_result['trial_count']}")
    
    logger.info(f"ETL Status: {etl_result['status']}")
    logger.info(f"ETL Duration: {etl_result['duration']}")
    if etl_result.get('total_trials'):
        logger.info(f"Total Trials: {etl_result['total_trials']}")
    if etl_result.get('processed_count') is not None:
        logger.info(f"Processed: {etl_result['processed_count']}")
        logger.info(f"Skipped: {etl_result['skipped_count']}")
        logger.info(f"Updated: {etl_result['updated_count']}")
        logger.info(f"Errors: {etl_result['error_count']}")
    logger.info("============================================")

@flow(
    name="clinical-trials-etl",
    description="Daily ETL pipeline for clinical trials data",
    log_prints=True
)
def clinical_trials_etl_flow(limit: Optional[int] = None) -> Dict[str, Any]:
    """
    Main flow that orchestrates the clinical trials ETL pipeline
    
    Args:
        limit: Maximum number of trials to download. If None, uses environment variable.
    
    Returns:
        Dict containing overall pipeline results
    """
    logger = get_run_logger()
    pipeline_start = datetime.now()
    
    logger.info("🚀 Starting Clinical Trials ETL Pipeline")
    logger.info(f"Pipeline start time: {pipeline_start}")
    
    try:
        # Step 1: Download data
        logger.info("📥 Step 1: Downloading clinical trials data")
        download_result = download_clinical_data(limit=limit)
        
        # Step 2: Process data
        logger.info("⚙️ Step 2: Processing data through ETL pipeline")
        etl_result = run_etl_pipeline(download_result=download_result)
        
        # Step 3: Send notification
        logger.info("📧 Step 3: Sending completion notification")
        send_notification(download_result, etl_result)
        
        pipeline_end = datetime.now()
        total_duration = pipeline_end - pipeline_start
        
        logger.info("✅ Clinical Trials ETL Pipeline completed successfully")
        logger.info(f"Total pipeline duration: {total_duration}")
        
        return {
            "status": "success",
            "total_duration": str(total_duration),
            "download_result": download_result,
            "etl_result": etl_result,
            "completion_time": pipeline_end.isoformat()
        }
        
    except Exception as e:
        pipeline_end = datetime.now()
        total_duration = pipeline_end - pipeline_start
        
        logger.error(f"❌ Clinical Trials ETL Pipeline failed: {e}")
        logger.error(f"Pipeline duration before failure: {total_duration}")
        
        return {
            "status": "failed",
            "error": str(e),
            "total_duration": str(total_duration),
            "failure_time": pipeline_end.isoformat()
        }

@flow(
    name="download-only",
    description="Download clinical trials data only",
    log_prints=True
)
def download_only_flow(limit: int = 10000) -> Dict[str, Any]:
    """
    Flow that only downloads clinical trials data without ETL processing
    
    Args:
        limit: Maximum number of trials to download
        
    Returns:
        Dict containing download results
    """
    logger = get_run_logger()
    logger.info(f"🚀 Starting Clinical Trials Download Only (limit: {limit})")
    
    try:
        download_result = download_clinical_data(limit=limit)
        logger.info("✅ Download completed successfully")
        return download_result
    except Exception as e:
        logger.error(f"❌ Download failed: {e}")
        raise

@flow(
    name="etl-only",
    description="ETL processing only (assumes data already downloaded)",
    log_prints=True
)
def etl_only_flow(data_file: Optional[str] = None) -> Dict[str, Any]:
    """
    Flow that only runs ETL processing on existing data
    
    Args:
        data_file: Path to data file. If None, uses environment variable.
    
    Returns:
        Dict containing ETL results
    """
    logger = get_run_logger()
    logger.info("🚀 Starting Clinical Trials ETL Only")
    
    try:
        etl_result = run_etl_pipeline(data_file=data_file)
        logger.info("✅ ETL processing completed successfully")
        return etl_result
    except Exception as e:
        logger.error(f"❌ ETL processing failed: {e}")
        raise

def create_scheduled_deployment():
    """Create and serve the scheduled deployment for continuous operation"""
    
    # Get configuration from environment
    schedule_interval_minutes = int(os.getenv('SCHEDULE_INTERVAL_MINUTES', '10'))
    download_limit = int(os.getenv('DOWNLOAD_LIMIT', '1000'))
    
    print(f"🕐 Setting up scheduled deployment:")
    print(f"   - Interval: {schedule_interval_minutes} minutes")
    print(f"   - Download limit: {download_limit} trials")
    print(f"   - Duplicate action: {os.getenv('DUPLICATE_ACTION', 'skip')}")
    
    # Use the newer Prefect 3.x serve approach with cron schedule
    cron_schedule = f"*/{schedule_interval_minutes} * * * *"  # Every N minutes
    
    # Serve the flow with the schedule
    clinical_trials_etl_flow.serve(
        name="clinical-trials-etl-scheduled",
        cron=cron_schedule,
        parameters={"limit": download_limit},
        tags=["etl", "clinical-trials", "scheduled", "continuous"],
        description=f"Scheduled ETL pipeline running every {schedule_interval_minutes} minutes"
    )

def main():
    """Main function to run flow"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Clinical Trials ETL with Prefect")
    parser.add_argument("--run-now", action="store_true", help="Run the full ETL flow immediately")
    parser.add_argument("--limit", type=int, help="Limit number of trials to download")
    parser.add_argument("--download-only", type=int, metavar="LIMIT", help="Run download only with specified limit")
    parser.add_argument("--etl-only", action="store_true", help="Run ETL only (assumes data exists)")
    parser.add_argument("--data-file", type=str, help="Path to data file for ETL processing")
    parser.add_argument("--serve", action="store_true", help="Start Prefect server")
    parser.add_argument("--schedule", action="store_true", help="Start scheduled continuous ETL (every 10 minutes)")
    
    args = parser.parse_args()
    
    if args.run_now:
        # Run the full ETL flow immediately
        result = clinical_trials_etl_flow(limit=args.limit)
        print(f"Flow completed with status: {result['status']}")
    elif args.download_only:
        # Run download only
        result = download_only_flow(limit=args.download_only)
        print(f"Download completed with status: {result['status']}")
    elif args.etl_only:
        # Run ETL only
        result = etl_only_flow(data_file=args.data_file)
        print(f"ETL completed with status: {result['status']}")
    elif args.schedule:
        # Start scheduled continuous operation
        print("🚀 Starting Continuous Clinical Trials ETL Pipeline")
        print("   Press Ctrl+C to stop")
        try:
            create_scheduled_deployment()
        except KeyboardInterrupt:
            print("\n⏹️  Stopping scheduled ETL pipeline")
    elif args.serve:
        # Start the Prefect server
        import subprocess
        subprocess.run(["prefect", "server", "start", "--host", "0.0.0.0"])
    else:
        print("Usage:")
        print("  python prefect_flows.py --run-now [--limit 1000]              # Run full ETL pipeline once")
        print("  python prefect_flows.py --download-only 1000                  # Download 1000 trials only")
        print("  python prefect_flows.py --etl-only [--data-file path]         # Process existing data only")
        print("  python prefect_flows.py --schedule                            # Start continuous ETL (every 10 min)")
        print("  python prefect_flows.py --serve                               # Start Prefect server")
        print("")
        print("Environment Variables:")
        print("  SCHEDULE_INTERVAL_MINUTES=10    # Schedule interval (default: 10)")
        print("  DOWNLOAD_LIMIT=1000             # Trials per run (default: 1000)")
        print("  DUPLICATE_ACTION=skip           # skip/update/error (default: skip)")

if __name__ == "__main__":
    main() 
