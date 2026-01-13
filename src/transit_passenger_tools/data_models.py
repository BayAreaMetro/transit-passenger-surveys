"""
Pydantic data models for transit passenger survey database schema.

This module defines the canonical schema for standardized transit survey data.
All enum fields use integer codes for storage while providing validation and labels.
"""

from datetime import date, datetime
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field

from transit_passenger_tools.codebook import (
    AccessMode,
    ClipperDetail,
    Direction,
    EgressMode,
    EnglishProficiency,
    FareCategory,
    FareMedium,
    Gender,
    Hispanic,
    HouseholdIncome,
    HouseholdSize,
    InterviewLanguage,
    LanguageAtHomeBinary,
    StudentStatus,
    SurveyType,
    TransferCount,
    TripPurpose,
    VehicleCount,
    Weekpart,
    WorkerCount,
    WorkStatus,
)


class SurveyResponse(BaseModel):
    """
    Canonical schema for transit passenger survey responses.

    Each row represents a single on-board survey intercept of a passenger
    boarding a transit vehicle. The schema includes trip details, demographics,
    geographic information, and survey metadata.
    """

    model_config = ConfigDict(
        use_enum_values=True,  # Store enums as integers in database
        validate_assignment=True,
        str_strip_whitespace=True,
    )

    # ========== Primary Key ==========
    unique_id: str = Field(
        ...,
        max_length=100,
        description="Unique record identifier (concatenation of ID, operator, year)",
    )

    # ========== Survey Identifiers ==========
    id: str = Field(..., max_length=50, description="Record ID for operator survey")
    operator: str = Field(..., max_length=50, description="Transit operator name")
    survey_year: int = Field(..., ge=1990, le=2100, description="Year survey conducted")
    canonical_operator: Optional[str] = Field(
        None, max_length=50, description="Standardized operator name"
    )
    survey_name: Optional[str] = Field(None, max_length=100, description="Survey name")

    # ========== Trip Information ==========
    route: Optional[str] = Field(None, max_length=50, description="Transit route")
    direction: Optional[Direction] = Field(None, description="Travel direction of surveyed vehicle")
    boardings: Optional[int] = Field(
        None, ge=1, le=10, description="Number of boardings in transit trip"
    )

    # ========== Access/Egress Modes ==========
    access_mode: Optional[AccessMode] = Field(
        None, description="Mode to access first transit encounter"
    )
    egress_mode: Optional[EgressMode] = Field(
        None, description="Mode to egress from last transit encounter"
    )
    immediate_access_mode: Optional[AccessMode] = Field(
        None, description="Access mode to surveyed vehicle"
    )
    immediate_egress_mode: Optional[EgressMode] = Field(
        None, description="Egress mode from surveyed vehicle"
    )

    # ========== Transfer Information ==========
    transfer_from: Optional[str] = Field(
        None, max_length=50, description="Operator immediately transferred from"
    )
    transfer_to: Optional[str] = Field(
        None, max_length=50, description="Operator immediately transferring to"
    )
    number_transfers_orig_board: Optional[TransferCount] = Field(
        None, description="Number of transfers from origin to survey boarding"
    )
    number_transfers_alight_dest: Optional[TransferCount] = Field(
        None, description="Number of transfers from survey alighting to destination"
    )
    first_route_before_survey_board: Optional[str] = Field(
        None, max_length=50, description="First route before survey boarding"
    )
    second_route_before_survey_board: Optional[str] = Field(
        None, max_length=50, description="Second route before survey boarding"
    )
    third_route_before_survey_board: Optional[str] = Field(
        None, max_length=50, description="Third route before survey boarding"
    )
    first_route_after_survey_alight: Optional[str] = Field(
        None, max_length=50, description="First route after survey alighting"
    )
    second_route_after_survey_alight: Optional[str] = Field(
        None, max_length=50, description="Second route after survey alighting"
    )
    third_route_after_survey_alight: Optional[str] = Field(
        None, max_length=50, description="Third route after survey alighting"
    )

    # ========== Technology/Path ==========
    survey_tech: Optional[str] = Field(
        None, max_length=50, description="Survey vehicle technology type"
    )
    first_board_tech: Optional[str] = Field(
        None, max_length=50, description="Vehicle technology type of first boarding"
    )
    last_alight_tech: Optional[str] = Field(
        None, max_length=50, description="Vehicle technology type of last alighting"
    )
    path_access: Optional[str] = Field(
        None, max_length=20, description="Aggregated access mode (drive/walk/missing)"
    )
    path_egress: Optional[str] = Field(
        None, max_length=20, description="Aggregated egress mode (drive/walk/missing)"
    )
    path_line_haul: Optional[str] = Field(
        None, max_length=20, description="Path line haul mode (COM/EXP/HVY/LOC/LRF)"
    )
    path_label: Optional[str] = Field(
        None, max_length=50, description="Full path (access + line haul + egress)"
    )

    # ========== Trip Purpose ==========
    orig_purp: Optional[TripPurpose] = Field(None, description="Origin trip purpose")
    dest_purp: Optional[TripPurpose] = Field(None, description="Destination trip purpose")
    tour_purp: Optional[TripPurpose] = Field(None, description="Tour purpose (derived)")
    trip_purp: Optional[TripPurpose] = Field(None, description="Trip purpose")

    # ========== Ancillary Variables (Computed Tour Logic) ==========
    at_work_prior_to_orig_purp: Optional[str] = Field(
        None, max_length=50, description="Was at work before surveyed trip"
    )
    at_work_after_dest_purp: Optional[str] = Field(
        None, max_length=50, description="Will be at work after surveyed trip"
    )
    at_school_prior_to_orig_purp: Optional[str] = Field(
        None, max_length=50, description="Was at school before surveyed trip"
    )
    at_school_after_dest_purp: Optional[str] = Field(
        None, max_length=50, description="Will be at school after surveyed trip"
    )

    # ========== Geographic - Lat/Lon Coordinates ==========
    orig_lat: Optional[float] = Field(None, ge=-90, le=90, description="Trip origin latitude")
    orig_lon: Optional[float] = Field(
        None, ge=-180, le=180, description="Trip origin longitude"
    )
    dest_lat: Optional[float] = Field(None, ge=-90, le=90, description="Trip destination latitude")
    dest_lon: Optional[float] = Field(
        None, ge=-180, le=180, description="Trip destination longitude"
    )
    home_lat: Optional[float] = Field(None, ge=-90, le=90, description="Home location latitude")
    home_lon: Optional[float] = Field(
        None, ge=-180, le=180, description="Home location longitude"
    )
    workplace_lat: Optional[float] = Field(
        None, ge=-90, le=90, description="Workplace location latitude"
    )
    workplace_lon: Optional[float] = Field(
        None, ge=-180, le=180, description="Workplace location longitude"
    )
    school_lat: Optional[float] = Field(
        None, ge=-90, le=90, description="School location latitude"
    )
    school_lon: Optional[float] = Field(
        None, ge=-180, le=180, description="School location longitude"
    )
    first_board_lat: Optional[float] = Field(
        None, ge=-90, le=90, description="First transit boarding location latitude"
    )
    first_board_lon: Optional[float] = Field(
        None, ge=-180, le=180, description="First transit boarding location longitude"
    )
    last_alight_lat: Optional[float] = Field(
        None, ge=-90, le=90, description="Last transit alighting location latitude"
    )
    last_alight_lon: Optional[float] = Field(
        None, ge=-180, le=180, description="Last transit alighting location longitude"
    )
    survey_board_lat: Optional[float] = Field(
        None, ge=-90, le=90, description="Survey vehicle boarding location latitude"
    )
    survey_board_lon: Optional[float] = Field(
        None, ge=-180, le=180, description="Survey vehicle boarding location longitude"
    )
    survey_alight_lat: Optional[float] = Field(
        None, ge=-90, le=90, description="Survey vehicle alighting location latitude"
    )
    survey_alight_lon: Optional[float] = Field(
        None, ge=-180, le=180, description="Survey vehicle alighting location longitude"
    )

    # ========== Geographic - Rail Stations ==========
    onoff_enter_station: Optional[str] = Field(
        None, max_length=100, description="Rail boarding station name"
    )
    onoff_exit_station: Optional[str] = Field(
        None, max_length=100, description="Rail alighting station name"
    )

    # ========== Geographic - Travel Model Zones (MAZ) ==========
    orig_maz: Optional[int] = Field(None, description="Origin MAZ (Travel Model geography)")
    dest_maz: Optional[int] = Field(None, description="Destination MAZ")
    home_maz: Optional[int] = Field(None, description="Home MAZ")
    workplace_maz: Optional[int] = Field(None, description="Workplace MAZ")
    school_maz: Optional[int] = Field(None, description="School MAZ")

    # ========== Geographic - Travel Model Zones (TAZ) ==========
    orig_taz: Optional[int] = Field(None, description="Origin TAZ (Travel Model geography)")
    dest_taz: Optional[int] = Field(None, description="Destination TAZ")
    home_taz: Optional[int] = Field(None, description="Home TAZ")
    workplace_taz: Optional[int] = Field(None, description="Workplace TAZ")
    school_taz: Optional[int] = Field(None, description="School TAZ")

    # ========== Geographic - Travel Model Zones (TAP) ==========
    first_board_tap: Optional[int] = Field(
        None, description="TAP of first boarding location (Travel Model geography)"
    )
    last_alight_tap: Optional[int] = Field(None, description="TAP of last alighting location")

    # ========== Geographic - Counties ==========
    home_county: Optional[str] = Field(None, max_length=50, description="Home county")
    workplace_county: Optional[str] = Field(None, max_length=50, description="Workplace county")
    school_county: Optional[str] = Field(None, max_length=50, description="School county")

    # ========== Fare Information ==========
    fare_medium: Optional[FareMedium] = Field(None, description="Payment method for fare")
    fare_category: Optional[FareCategory] = Field(
        None, description="Fare category (adult/youth/senior/disabled)"
    )
    clipper_detail: Optional[ClipperDetail] = Field(
        None, description="Specific Clipper payment type"
    )

    # ========== Time Information ==========
    depart_hour: Optional[int] = Field(
        None, ge=0, le=23, description="Hour leaving home prior to transit trip"
    )
    return_hour: Optional[int] = Field(
        None, ge=0, le=23, description="Hour next expected home after transit trip"
    )
    survey_time: Optional[str] = Field(
        None, max_length=20, description="Time survey conducted (may be start or completion)"
    )
    weekpart: Optional[Weekpart] = Field(None, description="Weekday or weekend")
    day_of_the_week: Optional[str] = Field(None, max_length=20, description="Day of week")
    day_part: Optional[str] = Field(
        None,
        max_length=20,
        description="Time period (Early AM/AM Peak/Midday/PM Peak/Evening/Night)",
    )
    field_start: Optional[date] = Field(None, description="First day of data collection")
    field_end: Optional[date] = Field(None, description="Last day of data collection")
    date_string: Optional[str] = Field(
        None, max_length=50, description="Interview date string (for parsing)"
    )
    time_string: Optional[str] = Field(
        None, max_length=50, description="Interview time string (for parsing)"
    )

    # ========== Person Demographics ==========
    approximate_age: Optional[int] = Field(
        None, ge=0, le=120, description="Approximate age (derived from year_born)"
    )
    year_born_four_digit: Optional[int] = Field(
        None, ge=1900, le=2025, description="Four-digit birth year"
    )
    gender: Optional[Gender] = Field(None, description="Gender identity")
    hispanic: Optional[Hispanic] = Field(None, description="Hispanic/Latino ethnicity")
    race: Optional[str] = Field(None, max_length=100, description="Race (text or code)")
    race_dmy_ind: Optional[int] = Field(
        None, ge=0, le=1, description="American Indian/Alaskan Native (dummy)"
    )
    race_dmy_asn: Optional[int] = Field(None, ge=0, le=1, description="Asian (dummy)")
    race_dmy_blk: Optional[int] = Field(None, ge=0, le=1, description="Black (dummy)")
    race_dmy_hwi: Optional[int] = Field(
        None, ge=0, le=1, description="Native Hawaiian/Pacific Islander (dummy)"
    )
    race_dmy_wht: Optional[int] = Field(None, ge=0, le=1, description="White (dummy)")
    race_other_string: Optional[str] = Field(None, max_length=100, description="Other race (text)")
    work_status: Optional[WorkStatus] = Field(None, description="Employment status")
    student_status: Optional[StudentStatus] = Field(None, description="Student enrollment status")
    eng_proficient: Optional[EnglishProficiency] = Field(
        None, description="English language proficiency"
    )

    # ========== Household Demographics ==========
    persons: Optional[HouseholdSize] = Field(None, description="Number of persons in household")
    workers: Optional[WorkerCount] = Field(None, description="Number of workers in household")
    vehicles: Optional[VehicleCount] = Field(None, description="Number of vehicles in household")
    household_income: Optional[HouseholdIncome] = Field(
        None, description="Annual household income bracket"
    )
    auto_suff: Optional[str] = Field(
        None, max_length=50, description="Auto sufficiency (sufficient/negotiating)"
    )
    language_at_home: Optional[str] = Field(
        None, max_length=100, description="Language spoken at home"
    )
    language_at_home_binary: Optional[LanguageAtHomeBinary] = Field(
        None, description="English only vs other language"
    )
    language_at_home_detail: Optional[str] = Field(
        None, max_length=100, description="Specific language spoken at home"
    )

    # ========== Survey Administration ==========
    survey_type: Optional[SurveyType] = Field(
        None, description="Survey administration method (CATI/paper/tablet)"
    )
    interview_language: Optional[InterviewLanguage] = Field(
        None, description="Language survey conducted in"
    )
    field_language: Optional[InterviewLanguage] = Field(
        None, description="Survey interview language (duplicate of interview_language)"
    )

    # ========== Weights (Base/Vendor-Provided) ==========
    weight: Optional[float] = Field(None, description="Boarding weight (unlinked boarding)")
    trip_weight: Optional[float] = Field(
        None, description="Linked trip weight (accounts for transfers)"
    )

    # ========== System Fields ==========
    created_at: datetime = Field(
        default_factory=datetime.utcnow, description="Record creation timestamp"
    )
    updated_at: Optional[datetime] = Field(None, description="Record last update timestamp")


class SurveyWeight(BaseModel):
    """
    Project-specific weight calculations for survey responses.

    This table stores derived/adjusted weights for different analysis scenarios,
    keeping them separate from the core survey data. Multiple weight schemes can
    coexist for different analysis purposes (e.g., scaled to different base years,
    adjusted with PopulationSim, etc.).
    """

    model_config = ConfigDict(validate_assignment=True)

    # ========== Link to Survey Response ==========
    unique_id: str = Field(
        ...,
        max_length=100,
        description="Foreign key to SurveyResponse.unique_id",
    )

    # ========== Weight Scheme Identifier ==========
    weight_scheme: str = Field(
        ...,
        max_length=50,
        description="Name of weighting scheme (e.g., '2015_ridership', 'popsim_adjusted')",
    )

    # ========== Weight Values ==========
    board_weight: Optional[float] = Field(
        None, description="Boarding weight for this scheme (unlinked)"
    )
    trip_weight: Optional[float] = Field(
        None, description="Trip weight for this scheme (linked, accounts for transfers)"
    )
    expansion_factor: Optional[float] = Field(
        None, description="Expansion factor applied to base weight"
    )

    # ========== Metadata ==========
    description: Optional[str] = Field(
        None, max_length=500, description="Description of this weighting methodology"
    )
    created_at: datetime = Field(
        default_factory=datetime.utcnow, description="Weight calculation timestamp"
    )
