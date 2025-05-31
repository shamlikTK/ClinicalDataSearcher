#!/usr/bin/env python3
"""
Clinical Trials ETL Scheduler

Schedules and runs the clinical trials data download and ETL pipeline jobs.
Runs daily at a specified time.
"""

import os
import sys
import time
import schedule
import logging
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
logging.basicConfig(
    level=getattr(logging, log_level),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('/app/logs/scheduler.log', mode='a') if os.path.exists('/app/logs') else logging.NullHandler()
    ]
)
logger = logging.getLogger(__name__)

class ClinicalTrialsScheduler:
    """Scheduler for clinical trials ETL pipeline"""
    
    def __init__(self):
        self.download_script = "download_clinical_data.py"
        self.etl_script = "etl_pipeline.py"
        self.schedule_time = os.getenv('SCHEDULE_TIME', '02:00')  # Default: 2 AM
        self.timezone = os.getenv('TIMEZONE', 'UTC')
        self.max_retries = int(os.getenv('MAX_RETRIES', '3'))
        self.retry_delay = int(os.getenv('RETRY_DELAY_MINUTES', '30'))  # 30 minutes
        
        # Ensure scripts exist
        self.validate_scripts()
        
    def validate_scripts(self):
        """Validate that required scripts exist"""
        scripts = [self.download_script, self.etl_script]
        for script in scripts:
            if not Path(script).exists():
                logger.error(f"Required script not found: {script}")
                sys.exit(1)
        logger.info("All required scripts found")
    
    def run_script(self, script_name: str, description: str) -> bool:
        """
        Run a Python script with proper error handling and logging
        
        Args:
            script_name: Name of the script to run
            description: Human-readable description for logging
            
        Returns:
            True if successful, False otherwise
        """
        logger.info(f"Starting {description}")
        logger.info(f"Running script: {script_name}")
        
        try:
            start_time = datetime.now()
            
            # Run the script using subprocess
            result = subprocess.run(
                [sys.executable, script_name],
                capture_output=True,
                text=True,
                timeout=3600  # 1 hour timeout
            )
            
            end_time = datetime.now()
            duration = end_time - start_time
            
            if result.returncode == 0:
                logger.info(f"{description} completed successfully in {duration}")
                if result.stdout:
                    logger.debug(f"{description} output: {result.stdout}")
                return True
            else:
                logger.error(f"{description} failed with return code {result.returncode}")
                if result.stderr:
                    logger.error(f"{description} error: {result.stderr}")
                if result.stdout:
                    logger.error(f"{description} output: {result.stdout}")
                return False
                
        except subprocess.TimeoutExpired:
            logger.error(f"{description} timed out after 1 hour")
            return False
        except Exception as e:
            logger.error(f"Unexpected error running {description}: {e}")
            return False
    
    def run_with_retry(self, script_name: str, description: str) -> bool:
        """
        Run a script with retry logic
        
        Args:
            script_name: Name of the script to run
            description: Human-readable description for logging
            
        Returns:
            True if successful after retries, False otherwise
        """
        for attempt in range(1, self.max_retries + 1):
            logger.info(f"Attempt {attempt}/{self.max_retries} for {description}")
            
            if self.run_script(script_name, description):
                return True
            
            if attempt < self.max_retries:
                logger.warning(f"{description} failed, retrying in {self.retry_delay} minutes")
                time.sleep(self.retry_delay * 60)  # Convert to seconds
            else:
                logger.error(f"{description} failed after {self.max_retries} attempts")
        
        return False
    
    def run_etl_pipeline(self):
        """Run the complete ETL pipeline: download then process"""
        logger.info("=" * 80)
        logger.info("STARTING SCHEDULED ETL PIPELINE RUN")
        logger.info("=" * 80)
        logger.info(f"Scheduled time: {self.schedule_time}")
        logger.info(f"Actual start time: {datetime.now()}")
        
        pipeline_start = datetime.now()
        success = True
        
        try:
            # Step 1: Download data
            logger.info("STEP 1: Downloading clinical trials data")
            if not self.run_with_retry(self.download_script, "Data Download"):
                logger.error("Data download failed, skipping ETL processing")
                success = False
            else:
                # Step 2: Process data (ETL)
                logger.info("STEP 2: Processing data through ETL pipeline")
                if not self.run_with_retry(self.etl_script, "ETL Processing"):
                    logger.error("ETL processing failed")
                    success = False
            
        except Exception as e:
            logger.error(f"Unexpected error in ETL pipeline: {e}")
            success = False
        
        finally:
            pipeline_end = datetime.now()
            total_duration = pipeline_end - pipeline_start
            
            logger.info("=" * 80)
            if success:
                logger.info("ETL PIPELINE COMPLETED SUCCESSFULLY")
            else:
                logger.error("ETL PIPELINE COMPLETED WITH ERRORS")
            logger.info(f"Total duration: {total_duration}")
            logger.info(f"Next scheduled run: {datetime.now() + timedelta(days=1)}")
            logger.info("=" * 80)
    
    def start_scheduler(self):
        """Start the scheduler and run indefinitely"""
        logger.info("=" * 80)
        logger.info("CLINICAL TRIALS ETL SCHEDULER STARTING")
        logger.info("=" * 80)
        logger.info(f"Schedule: Daily at {self.schedule_time}")
        logger.info(f"Download script: {self.download_script}")
        logger.info(f"ETL script: {self.etl_script}")
        logger.info(f"Max retries: {self.max_retries}")
        logger.info(f"Retry delay: {self.retry_delay} minutes")
        
        # Schedule the job
        schedule.every().day.at(self.schedule_time).do(self.run_etl_pipeline)
        
        # Calculate next run time
        next_run = schedule.next_run()
        logger.info(f"Next scheduled run: {next_run}")
        logger.info("Scheduler is now running. Press Ctrl+C to stop.")
        logger.info("=" * 80)
        
        try:
            while True:
                schedule.run_pending()
                time.sleep(60)  # Check every minute
                
        except KeyboardInterrupt:
            logger.info("Scheduler stopped by user")
        except Exception as e:
            logger.error(f"Scheduler error: {e}")
            raise

def main():
    """Main function to start the scheduler"""
    
    # Check if we want to run immediately for testing
    run_now = "--now" in sys.argv
    
    scheduler = ClinicalTrialsScheduler()
    
    if run_now:
        logger.info("Running ETL pipeline immediately (--now flag detected)")
        scheduler.run_etl_pipeline()
    else:
        # Start the scheduler
        scheduler.start_scheduler()

if __name__ == "__main__":
    main() 