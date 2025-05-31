import json
import re
from datetime import datetime
from typing import List, Dict, Any
from models import ClinicalTrial as PydanticClinicalTrial
import logging
from database import (Database, DatabaseConfig, ClinicalTrial, SearchVector, IdentificationModule, 
                     StatusModule, DescriptionModule, Condition, Keyword, Intervention, Location, Sponsor)
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from sqlalchemy import text
from dateutil import parser
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging from environment variables
log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
log_format = os.getenv('LOG_FORMAT', '%(asctime)s - %(name)s - %(levelname)s - %(message)s')

logging.basicConfig(
    level=getattr(logging, log_level),
    format=log_format
)
logger = logging.getLogger(__name__)

class ClinicalTrialsETL:
    """Clinical Trials ETL Pipeline"""

    def __init__(self, db_config: DatabaseConfig = None):
        """
        Initialize ETL pipeline
        
        Args:
            db_config: Database configuration. If None, will load from environment variables.
        """
        # Load database config from environment if not provided
        if db_config is None:
            db_config = self.load_db_config_from_env()
        
        self.db_config = db_config
        self.db = Database(db_config)
        self.duplicate_action = os.getenv('DUPLICATE_ACTION', 'skip')
        logger.info(f"ETL initialized with duplicate action: {self.duplicate_action}")

    @classmethod
    def from_environment(cls):
        """Create ETL instance using environment variables"""
        return cls()

    def load_db_config_from_env(self) -> DatabaseConfig:
        """Load database configuration from environment variables"""
        return DatabaseConfig(
            host=os.getenv('DB_HOST', 'postgres'),
            port=int(os.getenv('DB_PORT', '5432')),
            database=os.getenv('DB_NAME', 'clinical_trials_db'),
            user=os.getenv('DB_USER', 'postgres'),
            password=os.getenv('DB_PASSWORD', 'password')
        )

    def setup_database(self):
        """Create database tables if they don't exist"""
        self.db.create_tables()
        logger.info("Database tables created/verified")

    def load_data_from_file(self, data_file: str = None) -> List[Dict[str, Any]]:
        """
        Load clinical trials data from JSON file
        
        Args:
            data_file: Path to data file. If None, loads from environment variable.
            
        Returns:
            List of clinical trial data dictionaries
        """
        if data_file is None:
            data_file = os.getenv('DATA_FILE', '/app/data/clinical_trials.json')
        
        logger.info(f"Loading data from: {data_file}")
        
        if not os.path.exists(data_file):
            raise FileNotFoundError(f"Data file not found: {data_file}")
        
        try:
            with open(data_file, 'r') as f:
                trials_data = json.load(f)
            
            logger.info(f"Loaded {len(trials_data)} trials")
            return trials_data
        
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON data: {e}")
            raise
        except Exception as e:
            logger.error(f"Error loading data file: {e}")
            raise

    def run_etl(self, data_file: str = None) -> Dict[str, Any]:
        """
        Run the complete ETL process
        
        Args:
            data_file: Path to data file. If None, loads from environment variable.
            
        Returns:
            Dictionary with ETL results and statistics
        """
        start_time = datetime.now()
        
        logger.info("=" * 60)
        logger.info("Starting Clinical Trials ETL Pipeline")
        logger.info("=" * 60)
        logger.info(f"Database host: {self.db_config.host}")
        logger.info(f"Database name: {self.db_config.database}")
        logger.info(f"Duplicate action: {self.duplicate_action}")
        
        try:
            # Setup database
            self.setup_database()
            
            # Load data
            trials_data = self.load_data_from_file(data_file)
            
            # Process trials and get statistics
            stats = self.process_trials_with_stats(trials_data)
            
            end_time = datetime.now()
            duration = end_time - start_time
            
            # Prepare results
            results = {
                "status": "success",
                "duration": str(duration),
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "total_trials": len(trials_data),
                "processed_count": stats["processed_count"],
                "skipped_count": stats["skipped_count"],
                "updated_count": stats["updated_count"],
                "error_count": stats["error_count"],
                "data_file": data_file or os.getenv('DATA_FILE', '/app/data/clinical_trials.json')
            }
            
            logger.info("=" * 60)
            logger.info("ETL Pipeline completed successfully!")
            logger.info(f"Total duration: {duration}")
            logger.info(f"Processed: {stats['processed_count']}, Skipped: {stats['skipped_count']}, Updated: {stats['updated_count']}, Errors: {stats['error_count']}")
            logger.info("=" * 60)
            
            return results
            
        except Exception as e:
            end_time = datetime.now()
            duration = end_time - start_time
            
            error_results = {
                "status": "failed",
                "error": str(e),
                "duration": str(duration),
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat()
            }
            
            logger.error("=" * 60)
            logger.error(f"ETL Pipeline failed: {e}")
            logger.error(f"Duration before failure: {duration}")
            logger.error("=" * 60)
            
            raise RuntimeError(f"ETL Pipeline failed: {e}")

    def validate_data(self, data: Dict[str, Any]) -> PydanticClinicalTrial:
        """Validate data using Pydantic model"""
        try:
            return PydanticClinicalTrial(**data)
        except Exception as e:
            logger.error(f"Data validation error: {str(e)}")
            raise

    def parse_date(self, date_str: str) -> datetime:
        """Parse date string to datetime object"""
        if not date_str:
            return None
        try:
            return parser.parse(date_str)
        except:
            return None

    def parse_age_to_years(self, age_str: str) -> int:
        """Parse age string to years as integer"""
        if not age_str:
            return None
        
        # Remove common suffixes and extract number
        age_str = age_str.lower().strip()
        
        # Extract number from string like "18 Years", "65 years", "N/A", etc.
        match = re.search(r'(\d+)', age_str)
        if match:
            years = int(match.group(1))
            
            # Convert months to years if needed
            if 'month' in age_str:
                return max(1, years // 12)  # Convert months to years, minimum 1
            
            return years
        
        return None

    def extract_phase(self, design_info: dict) -> str:
        """Extract phase from design info"""
        if not design_info:
            return None
        
        phases = design_info.get('phases', [])
        if phases:
            return ', '.join(phases)
        
        return None

    def safe_get(self, obj, path, default=None):
        """Safely get value from object using dot notation for nested paths"""
        if obj is None:
            return default
            
        # Handle dot notation paths like 'status_module.overall_status'
        if '.' in str(path):
            keys = path.split('.')
            current = obj
            for key in keys:
                if current is None:
                    return default
                if isinstance(current, dict):
                    current = current.get(key)
                else:
                    current = getattr(current, key, None)
            return current if current is not None else default
        
        # Handle single key access
        if isinstance(obj, dict):
            return obj.get(path, default)
        return getattr(obj, path, default)

    def get_enum_value(self, obj, path, default=None):
        """Get enum value as string from nested path"""
        value = self.safe_get(obj, path, default)
        if hasattr(value, 'value'):
            return value.value
        return str(value) if value else default

    def get_date(self, obj, path, default=None):
        """Get and parse date from nested path"""
        date_str = self.safe_get(obj, path, default)
        return self.parse_date(date_str) if date_str else default

    def trial_exists(self, session: Session, nct_id: str) -> bool:
        """Check if a trial with the given NCT ID already exists"""
        existing = session.query(ClinicalTrial).filter(ClinicalTrial.nct_id == nct_id).first()
        return existing is not None

    def delete_existing_trial_data(self, session: Session, nct_id: str):
        """Delete all data related to an existing trial"""
        # Get trial ID first
        trial = session.query(ClinicalTrial).filter(ClinicalTrial.nct_id == nct_id).first()
        if trial:
            trial_id = trial.id
            
            # Delete in reverse order of foreign key dependencies
            session.query(SearchVector).filter(SearchVector.trial_id == trial_id).delete()
            session.query(Sponsor).filter(Sponsor.trial_id == trial_id).delete()
            session.query(Location).filter(Location.trial_id == trial_id).delete()
            session.query(Intervention).filter(Intervention.trial_id == trial_id).delete()
            session.query(Keyword).filter(Keyword.trial_id == trial_id).delete()
            session.query(Condition).filter(Condition.trial_id == trial_id).delete()
            session.query(DescriptionModule).filter(DescriptionModule.trial_id == trial_id).delete()
            session.query(StatusModule).filter(StatusModule.trial_id == trial_id).delete()
            session.query(IdentificationModule).filter(IdentificationModule.trial_id == trial_id).delete()
            session.query(ClinicalTrial).filter(ClinicalTrial.nct_id == nct_id).delete()

    def get_primary_location(self, protocol) -> dict:
        """Get the primary location from the trial data"""
        locations = self.safe_get(protocol, 'contacts_locations_module.locations', [])
        
        if not locations:
            return {}
        
        # For now, just take the first location as primary
        # You could add logic to determine which is truly "primary"
        first_location = locations[0] if locations else None
        if first_location:
            return {
                'city': first_location.city,
                'state': first_location.state,
                'country': first_location.country,
                'latitude': self.safe_get(first_location, 'geo_point.lat'),
                'longitude': self.safe_get(first_location, 'geo_point.lon')
            }
        
        return {}

    def create_clinical_trial(self, session: Session, trial: PydanticClinicalTrial) -> ClinicalTrial:
        """Create ClinicalTrial object from validated data"""
        if not trial.protocol_section:
            logger.warning("Protocol section is missing, using default values")
            return ClinicalTrial(
                nct_id="UNKNOWN",
                brief_title="Unknown Trial",
                overall_status="UNKNOWN",
                has_results=trial.has_results or False
            )

        # Use trial.protocol_section as the base object for safe_get
        p = trial.protocol_section
        
        # Get primary location
        primary_location = self.get_primary_location(p)

        return ClinicalTrial(
            nct_id=self.safe_get(p, 'identification_module.nctId', 'UNKNOWN'),
            brief_title=self.safe_get(p, 'identification_module.briefTitle', 'Unknown Trial'),
            official_title=self.safe_get(p, 'identification_module.officialTitle'),
            overall_status=self.get_enum_value(p, 'status_module.overall_status', 'UNKNOWN'),
            study_type=self.get_enum_value(p, 'design_module.study_type'),
            phase=self.extract_phase(self.safe_get(p, 'design_module.design_info')),
            start_date=self.get_date(p, 'status_module.start_date_struct.date'),
            completion_date=self.get_date(p, 'status_module.completion_date_struct.date'),
            enrollment_count=self.safe_get(p, 'design_module.enrollment_info.count'),
            lead_sponsor_name=self.safe_get(p, 'sponsor_collaborators_module.lead_sponsor.name'),
            lead_sponsor_class=self.safe_get(p, 'sponsor_collaborators_module.lead_sponsor.class_type'),
            healthy_volunteers=self.safe_get(p, 'eligibility_module.healthy_volunteers', False),
            min_age_years=self.parse_age_to_years(self.safe_get(p, 'eligibility_module.minimum_age')),
            max_age_years=self.parse_age_to_years(self.safe_get(p, 'eligibility_module.maximum_age')),
            primary_location_city=primary_location.get('city'),
            primary_location_state=primary_location.get('state'),
            primary_location_country=primary_location.get('country'),
            primary_location_lat=primary_location.get('latitude'),
            primary_location_lon=primary_location.get('longitude'),
            has_results=trial.has_results or False
        )

    def create_identification_module(self, session: Session, trial_id: int, protocol):
        """Create identification module record"""
        ident = self.safe_get(protocol, 'identification_module', {})
        if ident:
            identification = IdentificationModule(
                trial_id=trial_id,
                nct_id=self.safe_get(ident, 'nctId'),
                org_study_id=self.safe_get(ident, 'orgStudyIdInfo.id'),
                organization_full_name=self.safe_get(ident, 'organization.name'),
                organization_class=self.safe_get(ident, 'organization.class_type'),
                brief_title=self.safe_get(ident, 'briefTitle'),
                official_title=self.safe_get(ident, 'officialTitle'),
                acronym=self.safe_get(ident, 'acronym')
            )
            session.add(identification)

    def create_status_module(self, session: Session, trial_id: int, protocol):
        """Create status module record"""
        status = protocol.status_module if protocol.status_module else None
        if status:
            status_module = StatusModule(
                trial_id=trial_id,
                status_verified_date=self.get_date(status, 'status_verified_date'),
                overall_status=self.get_enum_value(status, 'overall_status'),
                has_expanded_access=self.safe_get(status, 'has_expanded_access', False),
                start_date=self.get_date(status, 'start_date_struct.date'),
                completion_date=self.get_date(status, 'completion_date_struct.date'),
                study_first_submit_date=self.get_date(status, 'study_first_submit_date'),
                last_update_submit_date=self.get_date(status, 'last_update_submit_date')
            )
            session.add(status_module)

    def create_description_module(self, session: Session, trial_id: int, protocol):
        """Create description module record"""
        description = protocol.description_module if protocol.description_module else None
        if description:
            desc_module = DescriptionModule(
                trial_id=trial_id,
                brief_summary=description.brief_summary,
                detailed_description=description.detailed_description
            )
            session.add(desc_module)

    def process_conditions(self, session: Session, trial_id: int, protocol):
        """Process and create condition records"""
        conditions = self.safe_get(protocol, 'conditions_module.conditions', [])
        for condition_name in conditions:
            if condition_name:
                condition = Condition(
                    trial_id=trial_id,
                    condition=condition_name,
                    condition_category=None,  # Could be extracted from condition classification
                    mesh_id=None  # Could be extracted if available
                )
                session.add(condition)

    def process_interventions(self, session: Session, trial_id: int, protocol):
        """Process and create intervention records"""
        interventions = self.safe_get(protocol, 'arms_interventions_module.interventions', [])
        for intervention_data in interventions:
            if intervention_data and intervention_data.get('type') and intervention_data.get('name'):
                intervention = Intervention(
                    trial_id=trial_id,
                    type=intervention_data['type'],
                    name=intervention_data['name'],
                    description=intervention_data.get('description'),
                    intervention_category=None  # Could be classified based on type
                )
                session.add(intervention)

    def process_locations(self, session: Session, trial_id: int, protocol):
        """Process and create location records"""
        locations = self.safe_get(protocol, 'contacts_locations_module.locations', [])
        for i, loc_data in enumerate(locations):
            if not loc_data:
                continue
                
            location = Location(
                trial_id=trial_id,
                facility=loc_data.facility,
                status=loc_data.status,
                city=loc_data.city,
                state=loc_data.state,
                country=loc_data.country,
                latitude=self.safe_get(loc_data, 'geo_point.lat'),
                longitude=self.safe_get(loc_data, 'geo_point.lon'),
                is_primary_location=(i == 0)  # First location is primary
            )
            session.add(location)

    def process_sponsors(self, session: Session, trial_id: int, protocol):
        """Process and create sponsor records"""
        lead_sponsor_name = self.safe_get(protocol, 'sponsor_collaborators_module.lead_sponsor.name')
        if lead_sponsor_name:
            sponsor = Sponsor(
                trial_id=trial_id,
                sponsor_type='lead_sponsor',
                name=lead_sponsor_name,
                class_name=self.safe_get(protocol, 'sponsor_collaborators_module.lead_sponsor.class_type')
            )
            session.add(sponsor)

    def create_tsvector_from_text(self, session: Session, text_content: str) -> str:
        """Create a tsvector from text content using PostgreSQL's to_tsvector function"""
        if not text_content:
            return None
        
        try:
            # Use PostgreSQL's to_tsvector function to create proper tsvector
            result = session.execute(
                text("SELECT to_tsvector('english', :content)"),
                {'content': text_content}
            ).scalar()
            return result
        except Exception as e:
            logger.warning(f"Failed to create tsvector from text: {e}")
            return None

    def create_search_vectors(self, session: Session, trial_id: int, protocol):
        """Create search vectors for the trial"""
        # Collect all searchable text
        conditions = self.safe_get(protocol, 'conditions_module.conditions', [])
        interventions = self.safe_get(protocol, 'arms_interventions_module.interventions', [])
        locations = self.safe_get(protocol, 'contacts_locations_module.locations', [])
        
        # Get trial data for title vector
        brief_title = self.safe_get(protocol, 'identification_module.briefTitle', '')
        official_title = self.safe_get(protocol, 'identification_module.officialTitle', '')
        title_text = f"{brief_title} {official_title}".strip()
        
        # Get description data for description vector
        brief_summary = self.safe_get(protocol, 'description_module.brief_summary', '')
        detailed_description = self.safe_get(protocol, 'description_module.detailed_description', '')
        description_text = f"{brief_summary} {detailed_description}".strip()
        
        # Prepare text content for vectors
        all_conditions = ', '.join(conditions) if conditions else None
        all_interventions = ', '.join([i.get('name', '') for i in interventions if i]) if interventions else None
        all_locations = ', '.join([l.city for l in locations if l and l.city]) if locations else None
        all_sponsors = self.safe_get(protocol, 'sponsor_collaborators_module.lead_sponsor.name')
        
        # Create tsvectors
        title_vector = self.create_tsvector_from_text(session, title_text)
        condition_vector = self.create_tsvector_from_text(session, all_conditions)
        intervention_vector = self.create_tsvector_from_text(session, all_interventions)
        location_vector = self.create_tsvector_from_text(session, all_locations)
        description_vector = self.create_tsvector_from_text(session, description_text)
        
        search_vector = SearchVector(
            trial_id=trial_id,
            # Populate tsvector columns
            title_vector=title_vector,
            condition_vector=condition_vector,
            intervention_vector=intervention_vector,
            location_vector=location_vector,
            description_vector=description_vector,
            # Keep denormalized text fields for convenience
            all_conditions=all_conditions,
            all_interventions=all_interventions,
            all_locations=all_locations,
            all_sponsors=all_sponsors,
            all_descriptions=description_text if description_text else None,
            vector_version=1,
            completeness_score=0.8,  # Could calculate based on available data
            term_count=len([x for x in [all_conditions, all_interventions, all_locations, all_sponsors, description_text] if x])
        )
        session.add(search_vector)

    def insert_trial(self, session: Session, trial: PydanticClinicalTrial):
        """Insert trial data into the database"""
        trial_record = None
        try:
            # Create main trial record
            trial_record = self.create_clinical_trial(session, trial)
            nct_id = trial_record.nct_id

            # Check if trial already exists
            if self.trial_exists(session, nct_id):
                if self.duplicate_action == 'skip':
                    logger.info(f"Trial {nct_id} already exists, skipping")
                    return
                elif self.duplicate_action == 'update':
                    logger.info(f"Trial {nct_id} already exists, updating")
                    self.delete_existing_trial_data(session, nct_id)
                elif self.duplicate_action == 'error':
                    raise ValueError(f"Trial {nct_id} already exists")

            # Insert the trial
            session.add(trial_record)
            session.flush()  # Get the trial ID
            trial_id = trial_record.id

            # Process related data if protocol section exists
            if trial.protocol_section:
                protocol = trial.protocol_section
                self.create_identification_module(session, trial_id, protocol)
                self.create_status_module(session, trial_id, protocol)
                self.create_description_module(session, trial_id, protocol)
                self.process_conditions(session, trial_id, protocol)
                self.process_interventions(session, trial_id, protocol)
                self.process_locations(session, trial_id, protocol)
                self.process_sponsors(session, trial_id, protocol)
                self.create_search_vectors(session, trial_id, protocol)

            session.commit()
            action = "updated" if self.trial_exists(session, nct_id) else "inserted"
            logger.info(f"Successfully {action} trial {nct_id}")

        except Exception as e:
            session.rollback()
            nct_id = trial_record.nct_id if trial_record else "UNKNOWN"
            logger.error(f"Error processing trial {nct_id}: {str(e)}")
            raise

    def process_trials_with_stats(self, trials_data: List[Dict[str, Any]]) -> Dict[str, int]:
        """
        Process multiple clinical trials and return statistics
        
        Args:
            trials_data: List of clinical trial data dictionaries
            
        Returns:
            Dictionary with processing statistics
        """
        session = self.db.get_session()
        processed_count = 0
        skipped_count = 0
        updated_count = 0
        error_count = 0
        
        try:
            for i, trial_data in enumerate(trials_data):
                try:
                    validated_trial = self.validate_data(trial_data)
                    
                    # Get NCT ID for duplicate checking
                    nct_id = self.safe_get(validated_trial.protocol_section, 'identification_module.nctId', 'UNKNOWN')
                    
                    if self.trial_exists(session, nct_id):
                        if self.duplicate_action == 'skip':
                            skipped_count += 1
                            logger.debug(f"Skipping duplicate trial {nct_id}")
                            continue
                        elif self.duplicate_action == 'update':
                            updated_count += 1
                    
                    self.insert_trial(session, validated_trial)
                    processed_count += 1
                    
                    # Log progress every 100 trials
                    if (i + 1) % 100 == 0:
                        logger.info(f"Processed {i + 1}/{len(trials_data)} trials")
                    
                except Exception as e:
                    error_count += 1
                    logger.error(f"Error processing trial {i+1}: {str(e)}")
                    continue
                    
            logger.info(f"Processing completed: {processed_count} processed, {skipped_count} skipped, {updated_count} updated, {error_count} errors")
            
            return {
                "processed_count": processed_count,
                "skipped_count": skipped_count,
                "updated_count": updated_count,
                "error_count": error_count
            }
        finally:
            session.close()

    def process_trials(self, trials_data: List[Dict[str, Any]]):
        """Process multiple clinical trials (legacy method for compatibility)"""
        stats = self.process_trials_with_stats(trials_data)
        return stats

def main():
    logger.info("Starting Clinical Trials ETL Pipeline")
    logger.info(f"Duplicate action: {os.getenv('DUPLICATE_ACTION', 'skip')}")

    # Initialize ETL pipeline
    etl = ClinicalTrialsETL()

    # Create database tables if they don't exist
    etl.setup_database()
    logger.info("Database tables created/verified")

    # Load and process data
    data_file = os.getenv('DATA_FILE', '/app/data/clinical_trials.json')
    logger.info(f"Loading data from: {data_file}")
    
    try:
        results = etl.run_etl(data_file)
        logger.info("ETL pipeline completed successfully")
        logger.info(json.dumps(results))
    
    except FileNotFoundError:
        logger.error(f"Data file not found: {data_file}")
        raise
    except Exception as e:
        logger.error(f"Error in ETL pipeline: {str(e)}")
        raise

if __name__ == "__main__":
    main()
