from pydantic import BaseModel, Field, validator
from typing import List, Optional, Dict, Any
from datetime import datetime, date
from enum import Enum

class StudyType(str, Enum):
    INTERVENTIONAL = "INTERVENTIONAL"
    OBSERVATIONAL = "OBSERVATIONAL"
    EXPANDED_ACCESS = "EXPANDED_ACCESS"

class OverallStatus(str, Enum):
    ACTIVE_NOT_RECRUITING = "ACTIVE_NOT_RECRUITING"
    COMPLETED = "COMPLETED"
    ENROLLING_BY_INVITATION = "ENROLLING_BY_INVITATION"
    NOT_YET_RECRUITING = "NOT_YET_RECRUITING"
    RECRUITING = "RECRUITING"
    SUSPENDED = "SUSPENDED"
    TERMINATED = "TERMINATED"
    WITHDRAWN = "WITHDRAWN"
    AVAILABLE = "AVAILABLE"
    NO_LONGER_AVAILABLE = "NO_LONGER_AVAILABLE"
    TEMPORARILY_NOT_AVAILABLE = "TEMPORARILY_NOT_AVAILABLE"
    APPROVED_FOR_MARKETING = "APPROVED_FOR_MARKETING"
    WITHHELD = "WITHHELD"

class Organization(BaseModel):
    name: Optional[str] = None
    class_type: Optional[str] = Field(None, alias="class")

class IdentificationModule(BaseModel):
    nct_id: Optional[str] = Field(None, alias="nctId")
    org_study_id: Optional[str] = Field(None, alias="orgStudyIdInfo")
    brief_title: Optional[str] = Field(None, alias="briefTitle")
    official_title: Optional[str] = Field(None, alias="officialTitle")
    organization: Optional[Organization] = None

class DateStruct(BaseModel):
    date: Optional[str] = None
    type: Optional[str] = None

class StatusModule(BaseModel):
    overall_status: Optional[OverallStatus] = Field(None, alias="overallStatus")
    status_verified_date: Optional[str] = Field(None, alias="statusVerifiedDate")
    start_date_struct: Optional[DateStruct] = Field(None, alias="startDateStruct")
    p_completion_date_struct: Optional[DateStruct] = Field(None, alias="primaryCompletionDateStruct")
    completion_date_struct: Optional[DateStruct] = Field(None, alias="completionDateStruct")
    study_first_submit_date: Optional[str] = Field(None, alias="studyFirstSubmitDate")
    study_first_post_date_struct: Optional[DateStruct] = Field(None, alias="studyFirstPostDateStruct")
    last_update_submit_date: Optional[str] = Field(None, alias="lastUpdateSubmitDate")
    last_update_post_date_struct: Optional[DateStruct] = Field(None, alias="lastUpdatePostDateStruct")
    has_expanded_access: Optional[bool] = Field(False, alias="hasExpandedAccess")

class ResponsibleParty(BaseModel):
    type: Optional[str] = None
    investigator_full_name: Optional[str] = Field(None, alias="investigatorFullName")
    investigator_title: Optional[str] = Field(None, alias="investigatorTitle")
    investigator_affiliation: Optional[str] = Field(None, alias="investigatorAffiliation")

class SponsorCollaboratorsModule(BaseModel):
    responsible_party: Optional[ResponsibleParty] = Field(None, alias="responsibleParty")
    lead_sponsor: Optional[Organization] = Field(None, alias="leadSponsor")

class OversightModule(BaseModel):
    oversight_has_dmc: Optional[bool] = Field(False, alias="oversightHasDmc")
    is_fda_regulated_drug: Optional[bool] = Field(False, alias="isFdaRegulatedDrug")
    is_fda_regulated_device: Optional[bool] = Field(False, alias="isFdaRegulatedDevice")

class DescriptionModule(BaseModel):
    brief_summary: Optional[str] = Field(None, alias="briefSummary")
    detailed_description: Optional[str] = Field(None, alias="detailedDescription")

class DesignModule(BaseModel):
    study_type: Optional[StudyType] = Field(None, alias="studyType")
    phases: Optional[List[str]] = None
    design_info: Optional[Dict[str, Any]] = Field(None, alias="designInfo")
    enrollment_info: Optional[Dict[str, Any]] = Field(None, alias="enrollmentInfo")

class Intervention(BaseModel):
    type: Optional[str] = None
    name: Optional[str] = None
    description: Optional[str] = None
    arm_group_labels: Optional[List[str]] = Field(None, alias="armGroupLabels")

class ArmGroup(BaseModel):
    label: Optional[str] = None
    type: Optional[str] = None
    description: Optional[str] = None
    intervention_names: Optional[List[str]] = Field(None, alias="interventionNames")

class Outcome(BaseModel):
    measure: Optional[str] = None
    description: Optional[str] = None
    time_frame: Optional[str] = Field(None, alias="timeFrame")

class OutcomesModule(BaseModel):
    primary_outcomes: Optional[List[Outcome]] = Field(None, alias="primaryOutcomes")
    secondary_outcomes: Optional[List[Outcome]] = Field(None, alias="secondaryOutcomes")

class EligibilityModule(BaseModel):
    eligibility_criteria: Optional[str] = Field(None, alias="eligibilityCriteria")
    healthy_volunteers: Optional[bool] = Field(False, alias="healthyVolunteers")
    sex: Optional[str] = None
    minimum_age: Optional[str] = Field(None, alias="minimumAge")
    maximum_age: Optional[str] = Field(None, alias="maximumAge")

class Contact(BaseModel):
    name: Optional[str] = None
    role: Optional[str] = None
    phone: Optional[str] = None
    email: Optional[str] = None
    affiliation: Optional[str] = None

class Location(BaseModel):
    facility: Optional[str] = None
    status: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    zip: Optional[str] = None
    country: Optional[str] = None
    geo_point: Optional[Dict[str, float]] = Field(None, alias="geoPoint")
    contacts: Optional[List[Contact]] = None

class ContactsLocationsModule(BaseModel):
    locations: Optional[List[Location]] = None

class ProtocolSection(BaseModel):
    identification_module: Optional[Dict[str, Any]] = Field(None, alias="identificationModule")
    status_module: Optional[StatusModule] = Field(None, alias="statusModule")
    design_module: Optional[DesignModule] = Field(None, alias="designModule")
    description_module: Optional[DescriptionModule] = Field(None, alias="descriptionModule")
    eligibility_module: Optional[EligibilityModule] = Field(None, alias="eligibilityModule")
    oversight_module: Optional[OversightModule] = Field(None, alias="oversightModule")
    outcomes_module: Optional[OutcomesModule] = Field(None, alias="outcomesModule")
    contacts_locations_module: Optional[ContactsLocationsModule] = Field(None, alias="contactsLocationsModule")
    sponsor_collaborators_module: Optional[SponsorCollaboratorsModule] = Field(None, alias="sponsorCollaboratorsModule")
    conditions_module: Optional[Dict[str, Any]] = Field(None, alias="conditionsModule")
    arms_interventions_module: Optional[Dict[str, Any]] = Field(None, alias="armsInterventionsModule")

class ClinicalTrial(BaseModel):
    protocol_section: Optional[ProtocolSection] = Field(None, alias="protocolSection")
    has_results: Optional[bool] = Field(False, alias="hasResults")

    class Config:
        allow_population_by_field_name = True

    @validator('protocol_section')
    def validate_protocol_section(cls, v):
        if isinstance(v, dict):
            required_fields = ['identificationModule', 'statusModule']
            for field in required_fields:
                if field not in v:
                    raise ValueError(f"Missing required field: {field}")
        return v 