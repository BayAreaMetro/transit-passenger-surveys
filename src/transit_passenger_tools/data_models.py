"""
Pydantic data models for transit passenger survey database schema.

This module defines the canonical schema for standardized transit survey data.
Enum fields use string values matching CSV data exactly (case-sensitive).
"""

from datetime import date, datetime
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field

from transit_passenger_tools.codebook import (
    AccessEgressMode,
    DayOfWeek,
    Direction,
    EnglishProficiency,
    FareCategory,
    FareMedium,
    FieldLanguage,
    Gender,
    Hispanic,
    HouseholdIncome,
    InterviewLanguage,
    Race,
    StudentStatus,
    SurveyType,
    TechnologyType,
    TransferOperator,
    TripPurpose,
    VehicleCount,
    Weekpart,
    WorkStatus,
)


class SurveyMetadata(BaseModel):
    """
    Survey-wide metadata and collection information.
    
    This table stores metadata that applies to an entire survey effort,
    separate from individual response records. One row per survey.
    """
    
    model_config = ConfigDict(
        use_enum_values=True,
        validate_assignment=True,
        str_strip_whitespace=True,
    )
    
    # ========== Composite Primary Key ==========
    survey_year: int = Field(..., ge=1990, le=2100, description="Year survey conducted")
    canonical_operator: str = Field(..., max_length=50, description="Standardized operator name")
    
    # ========== Survey Identification ==========
    survey_name: str = Field(..., max_length=100, description="Survey name")
    source: str = Field(..., max_length=100, description="Data source/provenance")
    survey_batch: str = Field(..., max_length=100, description="Survey batch identifier")
    
    # ========== Field Collection Dates ==========
    field_start: date = Field(..., description="First day of data collection")
    field_end: date = Field(..., description="Last day of data collection")
    
    # ========== Inflation Adjustment ==========
    inflation_year: str = Field(..., max_length=10, description="Year for inflation adjustment")
    
    # ========== System Fields ==========
    created_at: datetime = Field(
        default_factory=datetime.utcnow, description="Record creation timestamp"
    )
    updated_at: Optional[datetime] = Field(None, description="Record last update timestamp")


class SurveyResponse(BaseModel):
    """
    Canonical schema for transit passenger survey responses.

    Each row represents a single on-board survey intercept of a passenger
    boarding a transit vehicle. The schema includes trip details, demographics,
    geographic information, and survey metadata.
    
    Field Classification (Usage-Based Validation):
    - Tier 1 (Required): Core identifiers, survey metadata, basic demographics, trip purpose
      Fields use Type = Field(...) pattern and must be filled with codebook defaults if NULL
    - Tier 2 (Optional - Enriched): Geocoded/derived data added post-survey (GEOIDs, TAZ/MAZ, coordinates, distances)
      Fields use Optional[Type] = Field(...) to accept None → write NULL to database
    - Tier 3 (Optional): All other nullable fields (conditional questions, sparse responses, transfers)
      Fields use Optional[Type] = Field(...) to accept None → write NULL to database
    """

    model_config = ConfigDict(
        use_enum_values=True,  # Store enums as strings in database
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
    survey_year: int = Field(..., ge=1990, le=2100, description="Year survey conducted")
    canonical_operator: str = Field(..., max_length=50, description="Standardized operator name")
    survey_name: str = Field(..., max_length=100, description="Survey name")

    # ========== Trip Information ==========
    route: str = Field(..., max_length=50, description="Transit route")
    direction: Direction = Field(..., description="Travel direction of surveyed vehicle")
    boardings: Optional[int] = Field(
        ..., ge=1, le=10, description="Number of boardings in transit trip (Tier 3: Optional)"
    )

    # ========== Access/Egress Modes ==========
    access_mode: AccessEgressMode = Field(..., description="Mode to access first transit encounter")
    egress_mode: AccessEgressMode = Field(..., description="Mode to egress from last transit encounter")
    immediate_access_mode: AccessEgressMode = Field(
        ..., description="Access mode to surveyed vehicle"
    )
    immediate_egress_mode: AccessEgressMode = Field(
        ..., description="Egress mode from surveyed vehicle"
    )

    # ========== Transfer Information (Tier 3: Optional - transfers only) ==========
    transfer_from: Optional[TransferOperator] = Field(
        ..., description="Operator immediately transferred from (Tier 3: Optional)"
    )
    transfer_to: Optional[TransferOperator] = Field(..., description="Operator immediately transferring to (Tier 3: Optional)")

    first_route_before_survey_board: Optional[str] = Field(
        ..., max_length=50, description="First route before survey boarding (Tier 3: Optional)"
    )
    second_route_before_survey_board: Optional[str] = Field(
        ..., max_length=50, description="Second route before survey boarding (Tier 3: Optional)"
    )
    third_route_before_survey_board: Optional[str] = Field(
        ..., max_length=50, description="Third route before survey boarding (Tier 3: Optional)"
    )
    first_route_after_survey_alight: Optional[str] = Field(
        ..., max_length=50, description="First route after survey alighting (Tier 3: Optional)"
    )
    second_route_after_survey_alight: Optional[str] = Field(
        ..., max_length=50, description="Second route after survey alighting (Tier 3: Optional)"
    )
    third_route_after_survey_alight: Optional[str] = Field(
        ..., max_length=50, description="Third route after survey alighting (Tier 3: Optional)"
    )

    # ========== Transfer Operators and Technology (Tier 3: Optional - transfers only) ==========
    first_before_operator: Optional[str] = Field(..., max_length=100, description="First operator before survey board (Tier 3: Optional)")
    first_before_operator_detail: Optional[str] = Field(..., max_length=100, description="First operator detail before survey board (Tier 3: Optional)")
    first_before_technology: Optional[str] = Field(..., max_length=50, description="First technology before survey board (Tier 3: Optional)")
    second_before_operator: Optional[str] = Field(..., max_length=100, description="Second operator before survey board (Tier 3: Optional)")
    second_before_operator_detail: Optional[str] = Field(..., max_length=100, description="Second operator detail before survey board (Tier 3: Optional)")
    second_before_technology: Optional[str] = Field(..., max_length=50, description="Second technology before survey board (Tier 3: Optional)")
    third_before_operator: Optional[str] = Field(..., max_length=100, description="Third operator before survey board (Tier 3: Optional)")
    third_before_operator_detail: Optional[str] = Field(..., max_length=100, description="Third operator detail before survey board (Tier 3: Optional)")
    third_before_technology: Optional[str] = Field(..., max_length=50, description="Third technology before survey board (Tier 3: Optional)")
    first_after_operator: Optional[str] = Field(..., max_length=100, description="First operator after survey alight (Tier 3: Optional)")
    first_after_operator_detail: Optional[str] = Field(..., max_length=100, description="First operator detail after survey alight (Tier 3: Optional)")
    first_after_technology: Optional[str] = Field(..., max_length=50, description="First technology after survey alight (Tier 3: Optional)")
    second_after_operator: Optional[str] = Field(..., max_length=100, description="Second operator after survey alight (Tier 3: Optional)")
    second_after_operator_detail: Optional[str] = Field(..., max_length=100, description="Second operator detail after survey alight (Tier 3: Optional)")
    second_after_technology: Optional[str] = Field(..., max_length=50, description="Second technology after survey alight (Tier 3: Optional)")
    third_after_operator: Optional[str] = Field(..., max_length=100, description="Third operator after survey alight (Tier 3: Optional)")
    third_after_operator_detail: Optional[str] = Field(..., max_length=100, description="Third operator detail after survey alight (Tier 3: Optional)")
    third_after_technology: Optional[str] = Field(..., max_length=50, description="Third technology after survey alight (Tier 3: Optional)")

    # ========== Technology/Path ==========
    survey_tech: TechnologyType = Field(..., description="Survey vehicle technology type")
    first_board_tech: TechnologyType = Field(
        ..., description="Vehicle technology type of first boarding"
    )
    last_alight_tech: TechnologyType = Field(
        ..., description="Vehicle technology type of last alighting"
    )
    path_access: str = Field(
        ..., max_length=20, description="Aggregated access mode (drive/walk/missing)"
    )
    path_egress: str = Field(
        ..., max_length=20, description="Aggregated egress mode (drive/walk/missing)"
    )
    path_line_haul: str = Field(
        ..., max_length=20, description="Path line haul mode (COM/EXP/HVY/LOC/LRF)"
    )
    path_label: str = Field(..., max_length=50, description="Full path (access + line haul + egress)")

    # ========== Mode Presence Flags (Tier 3: Optional - can be NULL for older surveys) ==========
    commuter_rail_present: Optional[int] = Field(
        ..., ge=0, le=1, description="Tier 3: Commuter rail present in trip (binary flag)"
    )
    heavy_rail_present: Optional[int] = Field(
        ..., ge=0, le=1, description="Tier 3: Heavy rail present in trip (binary flag)"
    )
    express_bus_present: Optional[int] = Field(
        ..., ge=0, le=1, description="Tier 3: Express bus present in trip (binary flag)"
    )
    ferry_present: Optional[int] = Field(
        ..., ge=0, le=1, description="Tier 3: Ferry present in trip (binary flag)"
    )
    light_rail_present: Optional[int] = Field(
        ..., ge=0, le=1, description="Tier 3: Light rail present in trip (binary flag)"
    )

    # ========== Trip Purpose ==========
    orig_purp: TripPurpose = Field(..., description="Origin trip purpose")
    dest_purp: TripPurpose = Field(..., description="Destination trip purpose")
    tour_purp: TripPurpose = Field(..., description="Tour purpose (derived)")
    trip_purp: TripPurpose = Field(..., description="Trip purpose")

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

    # ========== Geographic - Lat/Lon Coordinates (Tier 2: Enriched) ==========
    orig_lat: Optional[float] = Field(..., ge=-90, le=90, description="Trip origin latitude (Tier 2: Enriched)")
    orig_lon: Optional[float] = Field(
        ..., ge=-180, le=180, description="Trip origin longitude (Tier 2: Enriched)"
    )
    dest_lat: Optional[float] = Field(..., ge=-90, le=90, description="Trip destination latitude (Tier 2: Enriched)")
    dest_lon: Optional[float] = Field(
        ..., ge=-180, le=180, description="Trip destination longitude (Tier 2: Enriched)"
    )
    home_lat: Optional[float] = Field(..., ge=-90, le=90, description="Home location latitude (Tier 2: Enriched)")
    home_lon: Optional[float] = Field(
        ..., ge=-180, le=180, description="Home location longitude (Tier 2: Enriched)"
    )
    workplace_lat: Optional[float] = Field(
        ..., ge=-90, le=90, description="Workplace location latitude (Tier 3: Optional - workers only)"
    )
    workplace_lon: Optional[float] = Field(
        ..., ge=-180, le=180, description="Workplace location longitude (Tier 3: Optional - workers only)"
    )
    school_lat: Optional[float] = Field(
        ..., ge=-90, le=90, description="School location latitude (Tier 3: Optional - students only)"
    )
    school_lon: Optional[float] = Field(
        ..., ge=-180, le=180, description="School location longitude (Tier 3: Optional - students only)"
    )
    first_board_lat: Optional[float] = Field(
        ..., ge=-90, le=90, description="First transit boarding location latitude (Tier 2: Enriched)"
    )
    first_board_lon: Optional[float] = Field(
        ..., ge=-180, le=180, description="First transit boarding location longitude (Tier 2: Enriched)"
    )
    last_alight_lat: Optional[float] = Field(
        ..., ge=-90, le=90, description="Last transit alighting location latitude (Tier 2: Enriched)"
    )
    last_alight_lon: Optional[float] = Field(
        ..., ge=-180, le=180, description="Last transit alighting location longitude (Tier 2: Enriched)"
    )
    survey_board_lat: Optional[float] = Field(
        ..., ge=-90, le=90, description="Survey vehicle boarding location latitude (Tier 2: Enriched)"
    )
    survey_board_lon: Optional[float] = Field(
        ..., ge=-180, le=180, description="Survey vehicle boarding location longitude (Tier 2: Enriched)"
    )
    survey_alight_lat: Optional[float] = Field(
        ..., ge=-90, le=90, description="Survey vehicle alighting location latitude (Tier 2: Enriched)"
    )
    survey_alight_lon: Optional[float] = Field(
        ..., ge=-180, le=180, description="Survey vehicle alighting location longitude (Tier 2: Enriched)"
    )

    # ========== Geographic - Rail Stations (Tier 3: Optional - rail only) ==========
    onoff_enter_station: Optional[str] = Field(
        ..., max_length=100, description="Rail boarding station name (Tier 3: Optional - rail only)"
    )
    onoff_exit_station: Optional[str] = Field(
        ..., max_length=100, description="Rail alighting station name (Tier 3: Optional - rail only)"
    )

    # ========== Geographic - Travel Model Zones (MAZ) (Tier 2: Enriched) ==========
    orig_maz: Optional[int] = Field(..., description="Origin MAZ (Travel Model geography) (Tier 2: Enriched)")
    dest_maz: Optional[int] = Field(..., description="Destination MAZ (Tier 2: Enriched)")
    home_maz: Optional[int] = Field(..., description="Home MAZ (Tier 2: Enriched)")
    workplace_maz: Optional[int] = Field(..., description="Workplace MAZ (Tier 3: Optional - workers only)")
    school_maz: Optional[int] = Field(..., description="School MAZ (Tier 3: Optional - students only)")

    # ========== Geographic - Travel Model Zones (TAZ) (Tier 2: Enriched) ==========
    orig_taz: Optional[int] = Field(..., description="Origin TAZ (Travel Model geography) (Tier 2: Enriched)")
    dest_taz: Optional[int] = Field(..., description="Destination TAZ (Tier 2: Enriched)")
    home_taz: Optional[int] = Field(..., description="Home TAZ (Tier 2: Enriched)")
    workplace_taz: Optional[int] = Field(..., description="Workplace TAZ (Tier 3: Optional - workers only)")
    school_taz: Optional[int] = Field(..., description="School TAZ (Tier 3: Optional - students only)")

    # ========== Geographic - Travel Model Zones (TAP) (Tier 2: Enriched) ==========
    first_board_tap: Optional[int] = Field(
        ..., description="TAP of first boarding location (Travel Model geography) (Tier 2: Enriched)"
    )
    last_alight_tap: Optional[int] = Field(..., description="TAP of last alighting location (Tier 2: Enriched)")

    # ========== Geographic - Counties (Tier 2/3: Enriched/Conditional) ==========
    home_county: Optional[str] = Field(..., max_length=50, description="Home county (Tier 2: Enriched)")
    workplace_county: Optional[str] = Field(..., max_length=50, description="Workplace county (Tier 3: Optional - workers only)")
    school_county: Optional[str] = Field(..., max_length=50, description="School county (Tier 3: Optional - students only)")

    # ========== Geographic - Census GEOIDs (PUMA) (Tier 2: Enriched) ==========
    orig_PUMA_GEOID: Optional[str] = Field(..., max_length=20, description="Origin PUMA GEOID (Tier 2: Enriched)")
    dest_PUMA_GEOID: Optional[str] = Field(..., max_length=20, description="Destination PUMA GEOID (Tier 2: Enriched)")
    home_PUMA_GEOID: Optional[str] = Field(..., max_length=20, description="Home PUMA GEOID (Tier 2: Enriched)")
    workplace_PUMA_GEOID: Optional[str] = Field(..., max_length=20, description="Workplace PUMA GEOID (Tier 3: Optional - workers only)")
    school_PUMA_GEOID: Optional[str] = Field(..., max_length=20, description="School PUMA GEOID (Tier 3: Optional - students only)")
    first_board_PUMA_GEOID: Optional[str] = Field(..., max_length=20, description="First boarding location PUMA GEOID (Tier 2: Enriched)")
    last_alight_PUMA_GEOID: Optional[str] = Field(..., max_length=20, description="Last alighting location PUMA GEOID (Tier 2: Enriched)")
    survey_board_PUMA_GEOID: Optional[str] = Field(..., max_length=20, description="Survey boarding location PUMA GEOID (Tier 2: Enriched)")
    survey_alight_PUMA_GEOID: Optional[str] = Field(..., max_length=20, description="Survey alighting location PUMA GEOID (Tier 2: Enriched)")

    # ========== Geographic - Census GEOIDs (County) (Tier 2: Enriched) ==========
    orig_county_GEOID: Optional[str] = Field(..., max_length=10, description="Origin county GEOID (Tier 2: Enriched)")
    dest_county_GEOID: Optional[str] = Field(..., max_length=10, description="Destination county GEOID (Tier 2: Enriched)")
    home_county_GEOID: Optional[str] = Field(..., max_length=10, description="Home county GEOID (Tier 2: Enriched)")
    workplace_county_GEOID: Optional[str] = Field(..., max_length=10, description="Workplace county GEOID (Tier 3: Optional - workers only)")
    school_county_GEOID: Optional[str] = Field(..., max_length=10, description="School county GEOID (Tier 3: Optional - students only)")
    first_board_county_GEOID: Optional[str] = Field(..., max_length=10, description="First boarding location county GEOID (Tier 2: Enriched)")
    last_alight_county_GEOID: Optional[str] = Field(..., max_length=10, description="Last alighting location county GEOID (Tier 2: Enriched)")
    survey_board_county_GEOID: Optional[str] = Field(..., max_length=10, description="Survey boarding location county GEOID (Tier 2: Enriched)")
    survey_alight_county_GEOID: Optional[str] = Field(..., max_length=10, description="Survey alighting location county GEOID (Tier 2: Enriched)")

    # ========== Geographic - Census GEOIDs (Tract) (Tier 2: Enriched) ==========
    orig_tract_GEOID: Optional[str] = Field(..., max_length=15, description="Origin census tract GEOID (Tier 2: Enriched)")
    dest_tract_GEOID: Optional[str] = Field(..., max_length=15, description="Destination census tract GEOID (Tier 2: Enriched)")
    home_tract_GEOID: Optional[str] = Field(..., max_length=15, description="Home census tract GEOID (Tier 2: Enriched)")
    workplace_tract_GEOID: Optional[str] = Field(..., max_length=15, description="Workplace census tract GEOID (Tier 3: Optional - workers only)")
    school_tract_GEOID: Optional[str] = Field(..., max_length=15, description="School census tract GEOID (Tier 3: Optional - students only)")
    first_board_tract_GEOID: Optional[str] = Field(..., max_length=15, description="First boarding location census tract GEOID (Tier 2: Enriched)")
    last_alight_tract_GEOID: Optional[str] = Field(..., max_length=15, description="Last alighting location census tract GEOID (Tier 2: Enriched)")
    survey_board_tract_GEOID: Optional[str] = Field(..., max_length=15, description="Survey boarding location census tract GEOID (Tier 2: Enriched)")
    survey_alight_tract_GEOID: Optional[str] = Field(..., max_length=15, description="Survey alighting location census tract GEOID (Tier 2: Enriched)")

    # ========== Geographic - Travel Model Zones (TM1 TAZ) (Tier 2: Enriched) ==========
    orig_tm1_taz: Optional[int] = Field(..., description="Origin TM1 TAZ (Tier 2: Enriched)")
    dest_tm1_taz: Optional[int] = Field(..., description="Destination TM1 TAZ (Tier 2: Enriched)")
    home_tm1_taz: Optional[int] = Field(..., description="Home TM1 TAZ (Tier 2: Enriched)")
    workplace_tm1_taz: Optional[int] = Field(..., description="Workplace TM1 TAZ (Tier 3: Optional - workers only)")
    school_tm1_taz: Optional[int] = Field(..., description="School TM1 TAZ (Tier 3: Optional - students only)")
    first_board_tm1_taz: Optional[str] = Field(..., max_length=20, description="First boarding location TM1 TAZ (Tier 2: Enriched)")
    last_alight_tm1_taz: Optional[str] = Field(..., max_length=20, description="Last alighting location TM1 TAZ (Tier 2: Enriched)")
    survey_board_tm1_taz: Optional[str] = Field(..., max_length=20, description="Survey boarding location TM1 TAZ (Tier 2: Enriched)")
    survey_alight_tm1_taz: Optional[str] = Field(..., max_length=20, description="Survey alighting location TM1 TAZ (Tier 2: Enriched)")

    # ========== Geographic - Travel Model Zones (TM2 TAZ) (Tier 2: Enriched) ==========
    orig_tm2_taz: Optional[str] = Field(..., max_length=20, description="Origin TM2 TAZ (Tier 2: Enriched)")
    dest_tm2_taz: Optional[str] = Field(..., max_length=20, description="Destination TM2 TAZ (Tier 2: Enriched)")
    home_tm2_taz: Optional[str] = Field(..., max_length=20, description="Home TM2 TAZ (Tier 2: Enriched)")
    workplace_tm2_taz: Optional[str] = Field(..., max_length=20, description="Workplace TM2 TAZ (Tier 3: Optional - workers only)")
    school_tm2_taz: Optional[str] = Field(..., max_length=20, description="School TM2 TAZ (Tier 3: Optional - students only)")
    first_board_tm2_taz: Optional[str] = Field(..., max_length=20, description="First boarding location TM2 TAZ (Tier 2: Enriched)")
    last_alight_tm2_taz: Optional[str] = Field(..., max_length=20, description="Last alighting location TM2 TAZ (Tier 2: Enriched)")
    survey_board_tm2_taz: Optional[str] = Field(..., max_length=20, description="Survey boarding location TM2 TAZ (Tier 2: Enriched)")
    survey_alight_tm2_taz: Optional[str] = Field(..., max_length=20, description="Survey alighting location TM2 TAZ (Tier 2: Enriched)")

    # ========== Geographic - Travel Model Zones (TM2 MAZ) (Tier 2: Enriched) ==========
    orig_tm2_maz: Optional[str] = Field(..., max_length=20, description="Origin TM2 MAZ (Tier 2: Enriched)")
    dest_tm2_maz: Optional[str] = Field(..., max_length=20, description="Destination TM2 MAZ (Tier 2: Enriched)")
    home_tm2_maz: Optional[str] = Field(..., max_length=20, description="Home TM2 MAZ (Tier 2: Enriched)")
    workplace_tm2_maz: Optional[str] = Field(..., max_length=20, description="Workplace TM2 MAZ (Tier 3: Optional - workers only)")
    school_tm2_maz: Optional[str] = Field(..., max_length=20, description="School TM2 MAZ (Tier 3: Optional - students only)")
    first_board_tm2_maz: Optional[str] = Field(..., max_length=20, description="First boarding location TM2 MAZ (Tier 2: Enriched)")
    last_alight_tm2_maz: Optional[str] = Field(..., max_length=20, description="Last alighting location TM2 MAZ (Tier 2: Enriched)")
    survey_board_tm2_maz: Optional[str] = Field(..., max_length=20, description="Survey boarding location TM2 MAZ (Tier 2: Enriched)")
    survey_alight_tm2_maz: Optional[str] = Field(..., max_length=20, description="Survey alighting location TM2 MAZ (Tier 2: Enriched)")

    # ========== Geographic - Resolution Level (Tier 2: Enriched) ==========
    orig_geo_level: Optional[str] = Field(..., max_length=20, description="Geographic resolution level for origin (Tier 2: Enriched)")
    dest_geo_level: Optional[str] = Field(..., max_length=20, description="Geographic resolution level for destination (Tier 2: Enriched)")
    home_geo_level: Optional[str] = Field(..., max_length=20, description="Geographic resolution level for home (Tier 2: Enriched)")

    # ========== Distance Measures (Tier 2: Enriched) ==========
    distance_orig_dest: Optional[float] = Field(..., description="Distance from origin to destination (miles) (Tier 2: Enriched)")
    distance_board_alight: Optional[float] = Field(..., description="Distance from first board to last alight (miles) (Tier 2: Enriched)")
    distance_orig_first_board: Optional[float] = Field(..., description="Distance from origin to first board (miles) (Tier 2: Enriched)")
    distance_orig_survey_board: Optional[float] = Field(..., description="Distance from origin to survey board (miles) (Tier 2: Enriched)")
    distance_survey_alight_dest: Optional[float] = Field(..., description="Distance from survey alight to destination (miles) (Tier 2: Enriched)")
    distance_last_alight_dest: Optional[float] = Field(..., description="Distance from last alight to destination (miles) (Tier 2: Enriched)")

    # ========== Fare Information ==========
    fare_medium: FareMedium = Field(..., description="Payment method for fare")
    fare_category: FareCategory = Field(
        ..., description="Fare category (adult/youth/senior/disabled)"
    )

    # ========== Time Information (Tier 3: Optional) ==========
    depart_hour: Optional[int] = Field(
        ..., ge=0, le=23, description="Hour leaving home prior to transit trip (Tier 3: Optional)"
    )
    return_hour: Optional[int] = Field(
        ..., ge=0, le=23, description="Hour next expected home after transit trip (Tier 3: Optional)"
    )
    survey_time: Optional[str] = Field(
        ..., max_length=20, description="Tier 3: Time survey conducted (may be missing)"
    )
    weekpart: Weekpart = Field(..., description="Weekday or weekend")
    day_of_the_week: DayOfWeek = Field(..., description="Day of week")
    day_part: Optional[str] = Field(
        ...,
        max_length=20,
        description="Tier 3: Part of day (AM peak, midday, PM peak, evening, can be missing)"
    )
    date_string: str = Field(
        ..., max_length=50, description="Interview date string (for parsing)"
    )
    time_string: str = Field(
        ..., max_length=50, description="Interview time string (for parsing)"
    )

    # ========== Person Demographics ==========
    approximate_age: Optional[int] = Field(
        ..., ge=0, le=120, description="Tier 3: Approximate age (derived, can be missing)"
    )
    year_born_four_digit: Optional[int] = Field(
        None, ge=1900, le=2025, description="Four-digit birth year"
    )
    gender: Gender = Field(..., description="Gender identity")
    hispanic: Hispanic = Field(..., description="Hispanic/Latino ethnicity")
    race: Race = Field(..., description="Race")
    race_dmy_ind: Optional[int] = Field(
        None, ge=0, le=1, description="American Indian/Alaskan Native (dummy)"
    )
    race_dmy_asn: Optional[int] = Field(None, ge=0, le=1, description="Asian (dummy)")
    race_dmy_blk: Optional[int] = Field(None, ge=0, le=1, description="Black (dummy)")
    race_dmy_hwi: Optional[int] = Field(
        None, ge=0, le=1, description="Native Hawaiian/Pacific Islander (dummy)"
    )
    race_dmy_wht: Optional[int] = Field(None, ge=0, le=1, description="White (dummy)")
    race_other_string: Optional[str] = Field(..., max_length=100, description="Other race (text) (Tier 3: Optional)")
    work_status: WorkStatus = Field(..., description="Employment status")
    student_status: StudentStatus = Field(..., description="Student enrollment status")
    eng_proficient: EnglishProficiency = Field(..., description="English language proficiency")

    # ========== Household Demographics (Tier 3: Optional) ==========
    persons: Optional[int] = Field(..., description="Number of persons in household (Tier 3: Optional)")
    workers: Optional[int] = Field(..., description="Number of workers in household (Tier 3: Optional)")
    vehicles: Optional[VehicleCount] = Field(..., description="Number of vehicles in household (Tier 3: Optional)")
    household_income: Optional[HouseholdIncome] = Field(..., description="Annual household income bracket (Tier 3: Optional)")

    # ========== Income Continuous Values (Tier 2: Enriched - derived from bracket) ==========
    income_lower_bound: Optional[float] = Field(..., description="Lower bound of income bracket (dollars) (Tier 2: Enriched)")
    income_upper_bound: Optional[float] = Field(..., description="Upper bound of income bracket (dollars) (Tier 2: Enriched)")
    hh_income_nominal_continuous: Optional[float] = Field(..., description="Household income continuous value nominal dollars (Tier 2: Enriched)")
    hh_income_2023dollars_continuous: Optional[float] = Field(..., description="Household income continuous value in 2023 dollars (Tier 2: Enriched)")

    auto_suff: Optional[str] = Field(
        ..., max_length=50, description="Auto sufficiency (sufficient/negotiating) (Tier 3: Optional)"
    )
    language_at_home: Optional[str] = Field(..., max_length=100, description="Language spoken at home (Tier 3: Optional)")
    language_at_home_detail: Optional[str] = Field(
        None, max_length=100, description="Specific language spoken at home (Tier 3: Optional)"
    )

    # ========== Survey Administration ==========
    survey_type: SurveyType = Field(..., description="Survey administration method (CATI/paper/tablet)")
    interview_language: InterviewLanguage = Field(..., description="Language survey conducted in")
    field_language: FieldLanguage = Field(
        ..., description="Survey field language"
    )

    # ========== Derived Analysis Fields ==========
    autos_vs_workers: str = Field(..., max_length=50, description="Auto sufficiency category (autos vs workers)")
    vehicle_numeric_cat: Optional[str] = Field(
        ..., max_length=20, description="Tier 3: Vehicle count numeric category (can be missing)"
    )
    worker_numeric_cat: Optional[str] = Field(
        ..., max_length=20, description="Tier 3: Worker count numeric category (can be missing)"
    )
    tour_purp_case: Optional[str] = Field(
        ..., max_length=50, description="Tier 3: Tour purpose case for analysis (can be missing)"
    )
    time_period: str = Field(..., max_length=20, description="Time period classification")
    transit_type: str = Field(..., max_length=50, description="Transit type classification")
    transfers_surveyed: Optional[str] = Field(
        ..., max_length=20, description="Tier 3: Number of transfers surveyed (can be missing)"
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
    weight: float = Field(
        ..., description="Boarding weight for this scheme (unlinked)"
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
