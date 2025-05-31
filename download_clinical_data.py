#!/usr/bin/env python3
"""
Clinical Trials Data Downloader

Downloads clinical trial data from ClinicalTrials.gov API and saves it to the configured data file.
"""

import os
import json
import requests
import logging
from pathlib import Path
from typing import Dict, Any
from dotenv import load_dotenv
import time

# Load environment variables
load_dotenv()

# Configure logging
log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
logging.basicConfig(
    level=getattr(logging, log_level),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ClinicalTrialsDownloader:
    """Downloads clinical trial data from ClinicalTrials.gov API"""
    
    def __init__(self, data_file: str = None):
        """
        Initialize the downloader
        
        Args:
            data_file: Path to save data. If None, loads from environment variable.
        """
        self.api_url = "https://clinicaltrials.gov/api/int/studies/download"
        self.data_file = data_file or os.getenv('DATA_FILE', '/app/data/clinical_trials.json')
        self.timeout = 300  # 5 minutes timeout for large downloads
        
    @classmethod
    def from_environment(cls):
        """Create downloader instance using environment variables"""
        return cls()
        
    def download_data(self, limit: int = 10000, format: str = "json", sort: str = "@relevance") -> Dict[str, Any]:
        """
        Download clinical trial data from the API
        
        Args:
            limit: Maximum number of trials to download
            format: Data format (json, csv, etc.)
            sort: Sort criteria
            
        Returns:
            Downloaded data as dictionary
        """
        params = {
            'format': format,
            'sort': sort,
            'limit': limit
        }
        
        logger.info(f"Starting download from ClinicalTrials.gov API")
        logger.info(f"URL: {self.api_url}")
        logger.info(f"Parameters: {params}")
        logger.info(f"Expected file size: Large (may take several minutes)")
        
        try:
            start_time = time.time()
            
            # Make the request with streaming to handle large files
            response = requests.get(
                self.api_url, 
                params=params, 
                timeout=self.timeout,
                stream=True
            )
            response.raise_for_status()
            
            # Check content type
            content_type = response.headers.get('content-type', '')
            logger.info(f"Response content type: {content_type}")
            
            # Read the response content
            logger.info("Downloading data...")
            content = response.content
            
            # Parse JSON if it's JSON format
            if format.lower() == 'json':
                try:
                    data = json.loads(content.decode('utf-8'))
                    logger.info(f"Successfully parsed JSON data")
                    
                    # Log some basic statistics about the data
                    if isinstance(data, dict):
                        if 'studies' in data:
                            logger.info(f"Downloaded {len(data['studies'])} clinical trials")
                        elif isinstance(data, list):
                            logger.info(f"Downloaded {len(data)} clinical trials")
                        else:
                            logger.info(f"Downloaded data structure: {list(data.keys()) if hasattr(data, 'keys') else type(data)}")
                    elif isinstance(data, list):
                        logger.info(f"Downloaded {len(data)} clinical trials")
                    
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse JSON response: {e}")
                    logger.info("Saving raw content as fallback")
                    data = content.decode('utf-8')
            else:
                data = content.decode('utf-8')
            
            download_time = time.time() - start_time
            logger.info(f"Download completed in {download_time:.2f} seconds")
            
            return data
            
        except requests.exceptions.Timeout:
            logger.error(f"Download timed out after {self.timeout} seconds")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during download: {e}")
            raise
    
    def save_data(self, data: Dict[str, Any]) -> None:
        """
        Save downloaded data to the configured file path
        
        Args:
            data: Data to save
        """
        # Ensure the directory exists
        file_path = Path(self.data_file)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Saving data to: {self.data_file}")
        
        try:
            with open(self.data_file, 'w', encoding='utf-8') as f:
                if isinstance(data, (dict, list)):
                    json.dump(data, f, indent=2, ensure_ascii=False)
                else:
                    f.write(str(data))
            
            # Get file size for confirmation
            file_size = file_path.stat().st_size
            logger.info(f"Data saved successfully")
            logger.info(f"File size: {file_size / (1024*1024):.2f} MB")
            
        except Exception as e:
            logger.error(f"Failed to save data: {e}")
            raise
    
    def download_and_save(self, limit: int = 10000) -> Dict[str, Any]:
        """
        Download clinical trial data and save it to the configured file
        
        Args:
            limit: Maximum number of trials to download
            
        Returns:
            Dictionary with download results and metadata
        """
        try:
            start_time = time.time()
            
            logger.info("=" * 60)
            logger.info("Clinical Trials Data Downloader")
            logger.info("=" * 60)
            logger.info(f"Target file: {self.data_file}")
            logger.info(f"Limit: {limit} trials")
            
            # Download the data
            data = self.download_data(limit=limit)
            
            # Save the data
            self.save_data(data)
            
            end_time = time.time()
            duration = end_time - start_time
            
            # Get file size
            file_size_mb = None
            if Path(self.data_file).exists():
                file_size = Path(self.data_file).stat().st_size / (1024*1024)
                file_size_mb = file_size
            
            # Determine trial count
            trial_count = 0
            if isinstance(data, list):
                trial_count = len(data)
            elif isinstance(data, dict) and 'studies' in data:
                trial_count = len(data['studies'])
            
            results = {
                "status": "success",
                "duration": f"{duration:.2f} seconds",
                "file_size_mb": file_size_mb,
                "trial_count": trial_count,
                "limit": limit,
                "data_file": self.data_file
            }
            
            logger.info("=" * 60)
            logger.info("Download completed successfully!")
            logger.info(f"Duration: {duration:.2f} seconds")
            logger.info(f"File size: {file_size_mb:.2f} MB" if file_size_mb else "File size: Unknown")
            logger.info(f"Trials downloaded: {trial_count}")
            logger.info("=" * 60)
            
            return results
            
        except Exception as e:
            end_time = time.time()
            duration = end_time - start_time
            
            logger.error("=" * 60)
            logger.error(f"Download failed: {e}")
            logger.error(f"Duration before failure: {duration:.2f} seconds")
            logger.error("=" * 60)
            
            raise RuntimeError(f"Download failed: {e}")

def main():
    """Main function to run the downloader"""
    downloader = ClinicalTrialsDownloader()
    
    # You can customize the limit here
    limit = int(os.getenv('DOWNLOAD_LIMIT', '10000'))
    
    try:
        downloader.download_and_save(limit=limit)
    except KeyboardInterrupt:
        logger.info("Download interrupted by user")
    except Exception as e:
        logger.error(f"Download failed: {e}")
        exit(1)

if __name__ == "__main__":
    main() 