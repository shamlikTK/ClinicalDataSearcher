from sqlalchemy import create_engine, Column, Integer, String, Boolean, Date, ForeignKey, JSON, Text, ARRAY, Float, DECIMAL, TIMESTAMP
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.dialects.postgresql import TSVECTOR
from datetime import datetime
from typing import Optional
from dataclasses import dataclass

Base = declarative_base()

class ClinicalTrial(Base):
    __tablename__ = 'clinical_trials'

    id = Column(Integer, primary_key=True)
    nct_id = Column(String(20), unique=True, nullable=False)
    
    # Core trial data
    brief_title = Column(Text)
    official_title = Column(Text)
    overall_status = Column(String(50))
    study_type = Column(String(50))
    phase = Column(String(20))
    start_date = Column(Date)
    completion_date = Column(Date)
    enrollment_count = Column(Integer)
    lead_sponsor_name = Column(String(500))
    lead_sponsor_class = Column(String(50))
    healthy_volunteers = Column(Boolean)
    min_age_years = Column(Integer)
    max_age_years = Column(Integer)
    
    # Geographic (single primary location)
    primary_location_city = Column(String(100))
    primary_location_state = Column(String(100))
    primary_location_country = Column(String(100))
    primary_location_lat = Column(DECIMAL(10, 8))
    primary_location_lon = Column(DECIMAL(11, 8))
    
    # Metadata
    has_results = Column(Boolean, default=False)
    created_at = Column(TIMESTAMP, default=datetime.utcnow)
    updated_at = Column(TIMESTAMP, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    search_vectors = relationship("SearchVector", back_populates="trial", cascade="all, delete-orphan", uselist=False)
    identification_modules = relationship("IdentificationModule", back_populates="trial", cascade="all, delete-orphan")
    status_modules = relationship("StatusModule", back_populates="trial", cascade="all, delete-orphan")
    description_modules = relationship("DescriptionModule", back_populates="trial", cascade="all, delete-orphan")
    conditions = relationship("Condition", back_populates="trial", cascade="all, delete-orphan")
    keywords = relationship("Keyword", back_populates="trial", cascade="all, delete-orphan")
    interventions = relationship("Intervention", back_populates="trial", cascade="all, delete-orphan")
    locations = relationship("Location", back_populates="trial", cascade="all, delete-orphan")
    sponsors = relationship("Sponsor", back_populates="trial", cascade="all, delete-orphan")

class SearchVector(Base):
    __tablename__ = 'search_vectors'

    id = Column(Integer, primary_key=True)
    trial_id = Column(Integer, ForeignKey('clinical_trials.id', ondelete='CASCADE'), unique=True)
    
    # Targeted search vector types using PostgreSQL tsvector
    title_vector = Column(TSVECTOR)
    condition_vector = Column(TSVECTOR)
    intervention_vector = Column(TSVECTOR)
    location_vector = Column(TSVECTOR)
    description_vector = Column(TSVECTOR)
    
    # Denormalized search fields
    all_conditions = Column(Text)
    all_interventions = Column(Text)
    all_keywords = Column(Text)
    all_locations = Column(Text)
    all_sponsors = Column(Text)
    all_descriptions = Column(Text)
    
    # Search metadata
    vector_version = Column(Integer, default=1)
    last_updated = Column(TIMESTAMP, default=datetime.utcnow)
    content_hash = Column(String(64))
    
    # Search quality metrics
    completeness_score = Column(DECIMAL(3, 2))
    term_count = Column(Integer)

    trial = relationship("ClinicalTrial", back_populates="search_vectors")

class IdentificationModule(Base):
    __tablename__ = 'identification_modules'

    id = Column(Integer, primary_key=True)
    trial_id = Column(Integer, ForeignKey('clinical_trials.id', ondelete='CASCADE'))
    nct_id = Column(String(20), nullable=False)
    org_study_id = Column(String(255))
    organization_full_name = Column(String(500))
    organization_class = Column(String(50))
    brief_title = Column(Text)
    official_title = Column(Text)
    acronym = Column(String(100))

    trial = relationship("ClinicalTrial", back_populates="identification_modules")

class StatusModule(Base):
    __tablename__ = 'status_modules'

    id = Column(Integer, primary_key=True)
    trial_id = Column(Integer, ForeignKey('clinical_trials.id', ondelete='CASCADE'))
    status_verified_date = Column(Date)
    overall_status = Column(String(50))
    has_expanded_access = Column(Boolean)
    start_date = Column(Date)
    completion_date = Column(Date)
    study_first_submit_date = Column(Date)
    last_update_submit_date = Column(Date)

    trial = relationship("ClinicalTrial", back_populates="status_modules")

class DescriptionModule(Base):
    __tablename__ = 'description_modules'

    id = Column(Integer, primary_key=True)
    trial_id = Column(Integer, ForeignKey('clinical_trials.id', ondelete='CASCADE'))
    brief_summary = Column(Text)
    detailed_description = Column(Text)

    trial = relationship("ClinicalTrial", back_populates="description_modules")

class Condition(Base):
    __tablename__ = 'conditions'

    id = Column(Integer, primary_key=True)
    trial_id = Column(Integer, ForeignKey('clinical_trials.id', ondelete='CASCADE'))
    condition = Column(String(255))
    condition_category = Column(String(100))
    mesh_id = Column(String(20))

    trial = relationship("ClinicalTrial", back_populates="conditions")

class Keyword(Base):
    __tablename__ = 'keywords'

    id = Column(Integer, primary_key=True)
    trial_id = Column(Integer, ForeignKey('clinical_trials.id', ondelete='CASCADE'))
    keyword = Column(String(255))
    keyword_type = Column(String(50))

    trial = relationship("ClinicalTrial", back_populates="keywords")

class Intervention(Base):
    __tablename__ = 'interventions'

    id = Column(Integer, primary_key=True)
    trial_id = Column(Integer, ForeignKey('clinical_trials.id', ondelete='CASCADE'))
    type = Column(String(50))
    name = Column(String(500))
    description = Column(Text)
    intervention_category = Column(String(100))

    trial = relationship("ClinicalTrial", back_populates="interventions")

class Location(Base):
    __tablename__ = 'locations'

    id = Column(Integer, primary_key=True)
    trial_id = Column(Integer, ForeignKey('clinical_trials.id', ondelete='CASCADE'))
    facility = Column(String(500))
    status = Column(String(50))
    city = Column(String(100))
    state = Column(String(100))
    country = Column(String(100))
    latitude = Column(DECIMAL(10, 8))
    longitude = Column(DECIMAL(11, 8))
    is_primary_location = Column(Boolean, default=False)

    trial = relationship("ClinicalTrial", back_populates="locations")

class Sponsor(Base):
    __tablename__ = 'sponsors'

    id = Column(Integer, primary_key=True)
    trial_id = Column(Integer, ForeignKey('clinical_trials.id', ondelete='CASCADE'))
    sponsor_type = Column(String(20))
    name = Column(String(500))
    class_name = Column('class', String(50))  # 'class' is reserved keyword

    trial = relationship("ClinicalTrial", back_populates="sponsors")

@dataclass
class DatabaseConfig:
    host: str
    port: int
    database: str
    user: str
    password: str

    def get_connection_string(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

class Database:
    def __init__(self, config: DatabaseConfig):
        self.engine = create_engine(config.get_connection_string())
        self.Session = sessionmaker(bind=self.engine)

    def create_tables(self):
        Base.metadata.create_all(self.engine)

    def get_session(self):
        return self.Session() 
