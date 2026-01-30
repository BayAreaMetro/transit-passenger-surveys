"""Pydantic data models for transit passenger survey database schema.

This module defines the canonical schema for standardized transit survey data.
Enum fields use string values matching CSV data exactly (case-sensitive).
"""

# ruff: noqa: E501, N815


from datetime import date, datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from transit_passenger_tools.codebook import (
    AccessEgressMode,
    AutoRatioCategory,
    DayOfWeek,
    DayPart,
    Direction,
    EnglishProficiency,
    FareCategory,
    FareMedium,
    Gender,
    Language,
    Race,
    StudentStatus,
    SurveyType,
    TechnologyType,
    TransferOperator,
    TripPurpose,
    Weekpart,
    WorkStatus,
)

# TODO: Since we have changed the data model, we may need to re-create the database and modify the backfill script

class SurveyMetadata(BaseModel):
    """Survey-wide metadata and collection information.

    This table stores metadata that applies to an entire survey effort,
    separate from individual response records. One row per survey.
    """

    model_config = ConfigDict(
        use_enum_values=True,
        validate_assignment=True,
        str_strip_whitespace=True,
    )

    # ========== Primary Key ==========
    survey_id: str = Field(..., max_length=100, description="Unique survey identifier (operator_year format)")

    # ========== Survey Identifiers ==========
    survey_year: int = Field(..., ge=1990, le=2100, description="Year survey conducted")
    canonical_operator: str = Field(..., max_length=50, description="Standardized operator name")

    # ========== Survey Identification ==========
    survey_name: str = Field(..., max_length=100, description="Survey name")
    source: str = Field(..., max_length=100, description="Data source/provenance")

    # ========== Field Collection Dates ==========
    field_start: date = Field(..., description="First day of data collection")
    field_end: date = Field(..., description="Last day of data collection")

    # ========== Inflation Adjustment ==========
    inflation_year: str | None = Field(None, max_length=10, description="Year for inflation adjustment (nullable)")

    # ========== Data Versioning and Lineage ==========
    data_version: int = Field(..., ge=1, description="Version number of the data files for this survey")
    data_commit: str = Field(..., max_length=40, description="Git commit SHA that generated this data version")
    data_hash: str = Field(..., max_length=64, description="SHA256 hash of data content for deduplication")
    ingestion_timestamp: datetime = Field(..., description="When this data version was ingested")
    processing_notes: str | None = Field(None, max_length=500, description="Notes about data processing/changes")


class CoreSurveyResponse(BaseModel):
    """Core survey data provided by vendors/operators.

    This model defines the minimum required schema that preprocessing scripts
    must produce. These fields are needed as inputs for pipeline derivations.
    
    Inheritance chain: CoreSurveyResponse → DerivedSurveyResponse → SurveyResponse
    """

    model_config = ConfigDict(
        use_enum_values=True,
        validate_assignment=True,
        str_strip_whitespace=True,
    )

    # ========== Primary Key ==========
    response_id: str = Field(..., max_length=100, description="Unique response record identifier")

    # ========== Foreign Keys ==========
    survey_id: str = Field(..., max_length=100, description="Foreign key to SurveyMetadata.survey_id")

    # ========== Survey Identifiers ==========
    original_id: str = Field(..., max_length=50, description="Original record ID from operator's survey")


    # ========== Trip Information ==========
    route: str | None = Field(..., max_length=200, description="Transit route")
    direction: Direction | None = Field(..., description="Travel direction of surveyed vehicle")

    # ========== Access/Egress Modes ==========
    access_mode: AccessEgressMode | None = Field(..., description="Mode to access first transit encounter")
    egress_mode: AccessEgressMode | None = Field(..., description="Mode to egress from last transit encounter")
    immediate_access_mode: AccessEgressMode | None = Field(..., description="Access mode to surveyed vehicle")
    immediate_egress_mode: AccessEgressMode | None = Field(..., description="Egress mode from surveyed vehicle")

    # ========== Transfer Information  ==========
    transfer_from: TransferOperator | None = Field(..., description="Operator immediately transferred from ")
    transfer_to: TransferOperator | None = Field(..., description="Operator immediately transferring to ")

    first_route_before_survey_board: str | None = Field(..., max_length=50, description="First route before survey boarding ")
    second_route_before_survey_board: str | None = Field(..., max_length=50, description="Second route before survey boarding ")
    third_route_before_survey_board: str | None = Field(..., max_length=50, description="Third route before survey boarding ")
    first_route_after_survey_alight: str | None = Field(..., max_length=50, description="First route after survey alighting ")
    second_route_after_survey_alight: str | None = Field(..., max_length=50, description="Second route after survey alighting ")
    third_route_after_survey_alight: str | None = Field(..., max_length=50, description="Third route after survey alighting ")

    # ========== Transfer Operators and Technology  ==========
    first_before_operator: str | None = Field(..., max_length=100, description="First operator before survey board ")
    first_before_operator_detail: str | None = Field(..., max_length=100, description="First operator detail before survey board ")
    first_before_technology: str | None = Field(..., max_length=50, description="First technology before survey board ")
    second_before_operator: str | None = Field(..., max_length=100, description="Second operator before survey board ")
    second_before_operator_detail: str | None = Field(..., max_length=100, description="Second operator detail before survey board ")
    second_before_technology: str | None = Field(..., max_length=50, description="Second technology before survey board ")
    third_before_operator: str | None = Field(..., max_length=100, description="Third operator before survey board ")
    third_before_operator_detail: str | None = Field(..., max_length=100, description="Third operator detail before survey board ")
    third_before_technology: str | None = Field(..., max_length=50, description="Third technology before survey board ")
    first_after_operator: str | None = Field(..., max_length=100, description="First operator after survey alight ")
    first_after_operator_detail: str | None = Field(..., max_length=100, description="First operator detail after survey alight ")
    first_after_technology: str | None = Field(..., max_length=50, description="First technology after survey alight ")
    second_after_operator: str | None = Field(..., max_length=100, description="Second operator after survey alight ")
    second_after_operator_detail: str | None = Field(..., max_length=100, description="Second operator detail after survey alight ")
    second_after_technology: str | None = Field(..., max_length=50, description="Second technology after survey alight ")
    third_after_operator: str | None = Field(..., max_length=100, description="Third operator after survey alight ")
    third_after_operator_detail: str | None = Field(..., max_length=100, description="Third operator detail after survey alight ")
    third_after_technology: str | None = Field(..., max_length=50, description="Third technology after survey alight ")

    # ========== Technology/Path ==========
    vehicle_tech: TechnologyType | None = Field(None, description="Technology type of vehicle being surveyed")
    first_board_tech: TechnologyType | None = Field(..., description="Vehicle technology type of first boarding")
    last_alight_tech: TechnologyType | None = Field(..., description="Vehicle technology type of last alighting")
    path_access: str | None = Field(..., max_length=20, description="Aggregated access mode")
    path_egress: str | None = Field(..., max_length=20, description="Aggregated egress mode")
    path_line_haul: str | None = Field(..., max_length=20, description="Path line haul mode")
    path_label: str | None = Field(..., max_length=50, description="Full path")

    # ========== Trip Purpose ==========
    orig_purp: TripPurpose | None = Field(..., description="Origin trip purpose")
    dest_purp: TripPurpose | None = Field(..., description="Destination trip purpose")
    trip_purp: TripPurpose | None = Field(..., description="Trip purpose")

    # ========== Geographic - Lat/Lon Coordinates ==========
    orig_lat: float | None = Field(..., ge=-90, le=90, description="Trip origin latitude ")
    orig_lon: float | None = Field(..., ge=-180, le=180, description="Trip origin longitude ")
    dest_lat: float | None = Field(..., ge=-90, le=90, description="Trip destination latitude")
    dest_lon: float | None = Field(..., ge=-180, le=180, description="Trip destination longitude")
    home_lat: float | None = Field(..., ge=-90, le=90, description="Home location latitude")
    home_lon: float | None = Field(..., ge=-180, le=180, description="Home location longitude")
    workplace_lat: float | None = Field(..., ge=-90, le=90, description="Workplace location latitude")
    workplace_lon: float | None = Field(..., ge=-180, le=180, description="Workplace location longitude")
    school_lat: float | None = Field(..., ge=-90, le=90, description="School location latitude")
    school_lon: float | None = Field(..., ge=-180, le=180, description="School location longitude")
    first_board_lat: float | None = Field(..., ge=-90, le=90, description="First transit boarding location latitude")
    first_board_lon: float | None = Field(..., ge=-180, le=180, description="First transit boarding location longitude")
    last_alight_lat: float | None = Field(..., ge=-90, le=90, description="Last transit alighting location latitude")
    last_alight_lon: float | None = Field(..., ge=-180, le=180, description="Last transit alighting location longitude")
    survey_board_lat: float | None = Field(..., ge=-90, le=90, description="Survey vehicle boarding location latitude")
    survey_board_lon: float | None = Field(..., ge=-180, le=180, description="Survey vehicle boarding location longitude")
    survey_alight_lat: float | None = Field(..., ge=-90, le=90, description="Survey vehicle alighting location latitude")
    survey_alight_lon: float | None = Field(..., ge=-180, le=180, description="Survey vehicle alighting location longitude")

    # ========== Geographic - Rail Stations ==========
    onoff_enter_station: str | None = Field(..., max_length=100, description="Rail boarding station name")
    onoff_exit_station: str | None = Field(..., max_length=100, description="Rail alighting station name")

    # ========== Fare Information ==========
    fare_medium: FareMedium | None = Field(..., description="Payment method for fare")
    fare_category: FareCategory | None = Field(..., description="Fare category (adult/youth/senior/disabled)")

    # ========== Time Information ==========
    depart_hour: int | None = Field(..., ge=0, le=23, description="Hour leaving home prior to transit trip")
    return_hour: int | None = Field(..., ge=0, le=23, description="Hour next expected home after transit trip")
    survey_time: str | None = Field(..., max_length=20, description="Time survey conducted (may be missing)")
    day_of_the_week: DayOfWeek | None = Field(..., description="Day of week")

    # ========== Person Demographics ==========
    approximate_age: int | None = Field(..., ge=0, le=120, description="Approximate age")
    gender: Gender | None = Field(..., description="Gender identity")
    is_hispanic: bool | None = Field(..., description="True if Hispanic/Latino or of Spanish origin, False if not, None if unknown")
    race: Race | None = Field(..., description="Race")
    race_dmy_ind: int | None = Field(None, ge=0, le=1, description="American Indian/Alaskan Native (dummy)")
    race_dmy_asn: int | None = Field(None, ge=0, le=1, description="Asian (dummy)")
    race_dmy_blk: int | None = Field(None, ge=0, le=1, description="Black (dummy)")
    race_dmy_hwi: int | None = Field(None, ge=0, le=1, description="Native Hawaiian/Pacific Islander (dummy)")
    race_dmy_wht: int | None = Field(None, ge=0, le=1, description="White (dummy)")
    race_other_string: str | None = Field(..., max_length=100, description="Other race (text) ")
    work_status: WorkStatus | None = Field(..., description="Employment status")
    student_status: StudentStatus | None = Field(..., description="Student enrollment status")
    eng_proficient: EnglishProficiency | None = Field(..., description="English language proficiency")

    # ========== Household Demographics  ==========
    persons: int | None = Field(..., ge=0, description="Number of persons in household ")
    workers: int | None = Field(..., ge=0, description="Number of workers in household ")
    vehicles: int | None = Field(..., ge=0, description="Number of vehicles in household ")
    household_income: float | None = Field(..., ge=0, description="Annual household income (continuous, nominal dollars)")
    household_income_category: str | None = Field(None, max_length=100, description="Original household income category string")
    income_approximated: bool | None = Field(None, description="True if household_income was calculated from bounds rather than provided directly")

    auto_to_workers_ratio: AutoRatioCategory | None = Field(..., description="Auto sufficiency (vehicle to worker ratio)")
    language_at_home: str | None = Field(..., max_length=100, description="Language spoken at home ")
    language_at_home_detail: str | None = Field(None, max_length=100, description="Specific language spoken at home ")

    # ========== Survey Administration ==========
    survey_type: SurveyType | None = Field(..., description="Survey administration method (CATI/paper/tablet)")
    interview_language: Language | None = Field(..., description="Language survey conducted in")
    field_language: Language | None = Field(..., description="Survey field language")


class DerivedSurveyResponse(CoreSurveyResponse):
    """Survey response with pipeline-derived fields.

    Inherits all core vendor fields and adds fields calculated by the MTC
    processing pipeline (geocoding, tour logic, distances, zone assignments).
    
    Inheritance chain: CoreSurveyResponse → DerivedSurveyResponse → SurveyResponse
    """

    # ========== Time Classification (Derived) ==========
    day_part: DayPart | None = Field(None, description="Part of day (EARLY AM, AM PEAK, MIDDAY, PM PEAK, EVENING)")
    weekpart: Weekpart | None = Field(None, description="Weekday or weekend")
    time_period: str | None = Field(None, max_length=20, description="Time period classification")

    # ========== Trip Calculations ==========
    boardings: int | None = Field(None, ge=1, le=10, description="Number of boardings in transit trip (calculated from transfers)")
    transfers_surveyed: str | None = Field(None, max_length=20, description="Number of transfers surveyed")

    # ========== Tour Purpose Logic ==========
    tour_purp: TripPurpose | None = Field(None, description="Tour purpose (calculated from trip purposes + work/school status)")
    tour_purp_case: str | None = Field(None, max_length=50, description="Tour purpose case for analysis")
    at_work_prior_to_orig_purp: bool | None = Field(None, description="Was at work before surveyed trip")
    at_work_after_dest_purp: bool | None = Field(None, description="Will be at work after surveyed trip")
    at_school_prior_to_orig_purp: bool | None = Field(None, description="Was at school before surveyed trip")
    at_school_after_dest_purp: bool | None = Field(None, description="Will be at school after surveyed trip")

    # ========== Auto Sufficiency (Derived) ==========
    autos_vs_workers: str | None = Field(None, max_length=50, description="Auto sufficiency category (autos vs workers)")
    vehicle_numeric_cat: str | None = Field(None, max_length=20, description="Vehicle count numeric category")
    worker_numeric_cat: str | None = Field(None, max_length=20, description="Worker count numeric category")

    # ========== Transit Classification ==========
    transit_type: str | None = Field(None, max_length=50, description="Transit type classification")

    # ========== Mode Presence Flags (Derived) ==========
    commuter_rail_present: int | None = Field(None, ge=0, le=1, description="Commuter rail present in trip")
    heavy_rail_present: int | None = Field(None, ge=0, le=1, description="Heavy rail present in trip")
    express_bus_present: int | None = Field(None, ge=0, le=1, description="Express bus present in trip")
    ferry_present: int | None = Field(None, ge=0, le=1, description="Ferry present in trip")
    light_rail_present: int | None = Field(None, ge=0, le=1, description="Light rail present in trip")

    # ========== Income (Derived) ==========
    # NOTE: These fields are derived during preprocessing, not in standardization pipeline
    # Income binning/approximation should happen in preprocessing stage for historical surveys
    income_lower_bound: float | None = Field(None, description="Lower bound of income bracket (dollars) - for binned data only")
    income_upper_bound: float | None = Field(None, description="Upper bound of income bracket (dollars) - for binned data only")
    hh_income_nominal_continuous: float | None = Field(None, description="Household income continuous value nominal dollars")
    hh_income_2023dollars_continuous: float | None = Field(None, description="Household income continuous value in 2023 dollars")

    # ========== Demographics (Derived) ==========
    year_born_four_digit: int | None = Field(None, ge=1900, le=2100, description="Four-digit birth year (derived from age + survey year)")

    # ========== Distance Measures ==========
    distance_orig_dest: float | None = Field(None, description="Distance from origin to destination (miles)")
    distance_board_alight: float | None = Field(None, description="Distance from first board to last alight (miles)")
    distance_orig_first_board: float | None = Field(None, description="Distance from origin to first board (miles)")
    distance_orig_survey_board: float | None = Field(None, description="Distance from origin to survey board (miles)")
    distance_survey_alight_dest: float | None = Field(None, description="Distance from survey alight to destination (miles)")
    distance_last_alight_dest: float | None = Field(None, description="Distance from last alight to destination (miles)")

    # ========== Geographic - Travel Model Zones (MAZ) ==========
    orig_maz: int | None = Field(None, description="Origin MAZ (Travel Model geography)")
    dest_maz: int | None = Field(None, description="Destination MAZ")
    home_maz: int | None = Field(None, description="Home MAZ")
    workplace_maz: int | None = Field(None, description="Workplace MAZ")
    school_maz: int | None = Field(None, description="School MAZ")

    # ========== Geographic - Travel Model Zones (TAZ) ==========
    orig_taz: int | None = Field(None, description="Origin TAZ (Travel Model geography)")
    dest_taz: int | None = Field(None, description="Destination TAZ")
    home_taz: int | None = Field(None, description="Home TAZ")
    workplace_taz: int | None = Field(None, description="Workplace TAZ")
    school_taz: int | None = Field(None, description="School TAZ")

    # ========== Geographic - Travel Model Zones (TAP) ==========
    first_board_tap: int | None = Field(None, description="TAP of first boarding location (Travel Model geography)")
    last_alight_tap: int | None = Field(None, description="TAP of last alighting location")

    # ========== Geographic - Travel Model Zones (TM1 TAZ) ==========
    orig_tm1_taz: int | None = Field(None, description="Origin TM1 TAZ")
    dest_tm1_taz: int | None = Field(None, description="Destination TM1 TAZ")
    home_tm1_taz: int | None = Field(None, description="Home TM1 TAZ")
    workplace_tm1_taz: int | None = Field(None, description="Workplace TM1 TAZ")
    school_tm1_taz: int | None = Field(None, description="School TM1 TAZ")
    first_board_tm1_taz: int | None = Field(None, description="First boarding location TM1 TAZ")
    last_alight_tm1_taz: int | None = Field(None, description="Last alighting location TM1 TAZ")
    survey_board_tm1_taz: int | None = Field(None, description="Survey boarding location TM1 TAZ")
    survey_alight_tm1_taz: int | None = Field(None, description="Survey alighting location TM1 TAZ")

    # ========== Geographic - Travel Model Zones (TM2 TAZ) ==========
    orig_tm2_taz: int | None = Field(None, description="Origin TM2 TAZ")
    dest_tm2_taz: int | None = Field(None, description="Destination TM2 TAZ")
    home_tm2_taz: int | None = Field(None, description="Home TM2 TAZ")
    workplace_tm2_taz: int | None = Field(None, description="Workplace TM2 TAZ")
    school_tm2_taz: int | None = Field(None, description="School TM2 TAZ")
    first_board_tm2_taz: int | None = Field(None, description="First boarding location TM2 TAZ")
    last_alight_tm2_taz: int | None = Field(None, description="Last alighting location TM2 TAZ")
    survey_board_tm2_taz: int | None = Field(None, description="Survey boarding location TM2 TAZ")
    survey_alight_tm2_taz: int | None = Field(None, description="Survey alighting location TM2 TAZ")

    # ========== Geographic - Travel Model Zones (TM2 MAZ) ==========
    orig_tm2_maz: int | None = Field(None, description="Origin TM2 MAZ")
    dest_tm2_maz: int | None = Field(None, description="Destination TM2 MAZ")
    home_tm2_maz: int | None = Field(None, description="Home TM2 MAZ")
    workplace_tm2_maz: int | None = Field(None, description="Workplace TM2 MAZ")
    school_tm2_maz: int | None = Field(None, description="School TM2 MAZ")
    first_board_tm2_maz: int | None = Field(None, description="First boarding location TM2 MAZ")
    last_alight_tm2_maz: int | None = Field(None, description="Last alighting location TM2 MAZ")
    survey_board_tm2_maz: int | None = Field(None, description="Survey boarding location TM2 MAZ")
    survey_alight_tm2_maz: int | None = Field(None, description="Survey alighting location TM2 MAZ")

    # ========== Geographic - Counties ==========
    home_county: str | None = Field(None, max_length=50, description="Home county")
    workplace_county: str | None = Field(None, max_length=50, description="Workplace county")
    school_county: str | None = Field(None, max_length=50, description="School county")

    # ========== Geographic - Census GEOIDs (Tract) ==========
    orig_tract_GEOID: str | None = Field(None, max_length=15, description="Origin census tract GEOID")
    dest_tract_GEOID: str | None = Field(None, max_length=15, description="Destination census tract GEOID")
    home_tract_GEOID: str | None = Field(None, max_length=15, description="Home census tract GEOID")
    workplace_tract_GEOID: str | None = Field(None, max_length=15, description="Workplace census tract GEOID")
    school_tract_GEOID: str | None = Field(None, max_length=15, description="School census tract GEOID")
    first_board_tract_GEOID: str | None = Field(None, max_length=15, description="First boarding location census tract GEOID")
    last_alight_tract_GEOID: str | None = Field(None, max_length=15, description="Last alighting location census tract GEOID")
    survey_board_tract_GEOID: str | None = Field(None, max_length=15, description="Survey boarding location census tract GEOID")
    survey_alight_tract_GEOID: str | None = Field(None, max_length=15, description="Survey alighting location census tract GEOID")

    # ========== Geographic - Census GEOIDs (County) ==========
    orig_county_GEOID: str | None = Field(None, max_length=10, description="Origin county GEOID")
    dest_county_GEOID: str | None = Field(None, max_length=10, description="Destination county GEOID")
    home_county_GEOID: str | None = Field(None, max_length=10, description="Home county GEOID")
    workplace_county_GEOID: str | None = Field(None, max_length=10, description="Workplace county GEOID")
    school_county_GEOID: str | None = Field(None, max_length=10, description="School county GEOID")
    first_board_county_GEOID: str | None = Field(None, max_length=10, description="First boarding location county GEOID")
    last_alight_county_GEOID: str | None = Field(None, max_length=10, description="Last alighting location county GEOID")
    survey_board_county_GEOID: str | None = Field(None, max_length=10, description="Survey boarding location county GEOID")
    survey_alight_county_GEOID: str | None = Field(None, max_length=10, description="Survey alighting location county GEOID")

    # ========== Geographic - Census GEOIDs (PUMA) ==========
    orig_PUMA_GEOID: str | None = Field(None, max_length=20, description="Origin PUMA GEOID")
    dest_PUMA_GEOID: str | None = Field(None, max_length=20, description="Destination PUMA GEOID")
    home_PUMA_GEOID: str | None = Field(None, max_length=20, description="Home PUMA GEOID")
    workplace_PUMA_GEOID: str | None = Field(None, max_length=20, description="Workplace PUMA GEOID")
    school_PUMA_GEOID: str | None = Field(None, max_length=20, description="School PUMA GEOID")
    first_board_PUMA_GEOID: str | None = Field(None, max_length=20, description="First boarding location PUMA GEOID")
    last_alight_PUMA_GEOID: str | None = Field(None, max_length=20, description="Last alighting location PUMA GEOID")
    survey_board_PUMA_GEOID: str | None = Field(None, max_length=20, description="Survey boarding location PUMA GEOID")
    survey_alight_PUMA_GEOID: str | None = Field(None, max_length=20, description="Survey alighting location PUMA GEOID")

    # ========== Geographic - Resolution Level ==========
    orig_geo_level: str | None = Field(None, max_length=20, description="Geographic resolution level for origin")
    dest_geo_level: str | None = Field(None, max_length=20, description="Geographic resolution level for destination")
    home_geo_level: str | None = Field(None, max_length=20, description="Geographic resolution level for home")

    # ========== Vendor Pass-Through ==========
    extra_fields: dict[str, Any] | None = Field(None, description="Vendor-specific non-standard fields preserved for data lake")


class SurveyResponse(DerivedSurveyResponse):
    """Complete survey response schema for database storage.

    Inherits Core + Derived fields. This is the final schema for validated
    survey responses ready for database insertion.
    
    Inheritance chain: CoreSurveyResponse → DerivedSurveyResponse → SurveyResponse
    
    Currently identical to DerivedSurveyResponse but available for future
    optional validation fields that don't fit in Core or Derived.
    """



class SurveyWeight(BaseModel):
    """Project-specific weight calculations for survey responses.

    This table stores derived/adjusted weights for different analysis scenarios,
    keeping them separate from the core survey data. Multiple weight schemes can
    coexist for different analysis purposes (e.g., scaled to different base years,
    adjusted with PopulationSim, etc.).
    """

    model_config = ConfigDict(validate_assignment=True)

    # ========== Link to Survey Response ==========
    response_id: str = Field(..., max_length=100, description="Foreign key to SurveyResponse.response_id")

    # ========== Weight Scheme Identifier ==========
    weight_scheme: str = Field(..., max_length=50, description="Name of weighting scheme (e.g., '2015_ridership', 'popsim_adjusted')")

    # ========== Weight Values ==========
    boarding_weight: float | None = Field(None, description="Boarding weight for this scheme (unlinked)")
    trip_weight: float | None = Field(None, description="Trip weight for this scheme (linked, accounts for transfers)")

    # ========== Metadata ==========
    description: str | None = Field(None, max_length=500, description="Description of this weighting methodology")
