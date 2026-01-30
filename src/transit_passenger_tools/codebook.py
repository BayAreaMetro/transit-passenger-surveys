"""Enumerated types for transit passenger survey data.

This module defines all categorical value types used across surveys.
Values are standardized to sentence case for consistency.
"""

from enum import Enum

# ruff: noqa: S105

class AccessEgressMode(str, Enum):
    """Mode used to access transit from origin."""

    WALK = "Walk"
    BIKE = "Bike"
    PNR = "PNR"
    KNR = "KNR"
    TNC = "TNC"
    TRANSIT = "Transit"
    OTHER = "Other"


# NOTE: Standardize these values...
class Direction(str, Enum):
    """Direction of travel."""

    NORTHBOUND = "Northbound"
    SOUTHBOUND = "Southbound"
    EASTBOUND = "Eastbound"
    WESTBOUND = "Westbound"
    CLOCKWISE = "Clockwise"
    COUNTERCLOCKWISE = "Counterclockwise"
    INBOUND = "Inbound"
    OUTBOUND = "Outbound"
    LOOP = "Loop"
    NORTH = "North"
    SOUTH = "South"
    EAST = "East"
    WEST = "West"
    N_LOOP = "N. Loop"
    S_LOOP = "S. Loop"
    B = "B"
    ZERO = "0"


class DayOfWeek(str, Enum):
    """Day of the week."""

    MONDAY = "Monday"
    TUESDAY = "Tuesday"
    WEDNESDAY = "Wednesday"
    THURSDAY = "Thursday"
    FRIDAY = "Friday"
    SATURDAY = "Saturday"
    SUNDAY = "Sunday"


class Weekpart(str, Enum):
    """Day type when survey was conducted."""

    WEEKDAY = "Weekday"
    WEEKEND = "Weekend"


class DayPart(str, Enum):
    """Part of day for survey or trip timing."""

    EARLY_AM = "EARLY AM"
    AM_PEAK = "AM PEAK"
    MIDDAY = "MIDDAY"
    PM_PEAK = "PM PEAK"
    EVENING = "EVENING"

    def time_range(self) -> tuple[int, int] | None:
        """Get the hour range for this day part (closed intervals).
        
        Returns:
            Tuple of (start_hour, end_hour) inclusive, or None for EVENING/MISSING.
            EVENING covers 19:00-2:59 (wraps around midnight).
        """
        ranges = {
            DayPart.EARLY_AM: (3, 5),    # 3:00 - 5:59
            DayPart.AM_PEAK: (6, 9),     # 6:00 - 9:59
            DayPart.MIDDAY: (10, 14),    # 10:00 - 14:59
            DayPart.PM_PEAK: (15, 18),   # 15:00 - 18:59
            # EVENING: everything else (19-2)
        }
        return ranges.get(self)

    def to_period_code(self) -> str:
        """Convert to travel model period code by taking first letter(s).
        
        Returns:
            Period code string ("EA", "AM", "MD", "PM", "EV").
            
        Examples:
            >>> DayPart.EARLY_AM.to_period_code()
            'EA'
            >>> DayPart.AM_PEAK.to_period_code()
            'AM'
        """
        # Map each day part to its period code
        codes = {
            DayPart.EARLY_AM: "EA",
            DayPart.AM_PEAK: "AM",
            DayPart.MIDDAY: "MD",
            DayPart.PM_PEAK: "PM",
            DayPart.EVENING: "EV",
        }
        return codes.get(self)


class TransferOperator(str, Enum):
    """Transit operator for transfers."""

    AC_TRANSIT = "AC Transit"
    ACE = "ACE"
    AMTRAK = "Amtrak"
    BART = "BART"
    BLUE_GOLD_FERRY = "Blue Gold Ferry"
    BAY_AREA_SHUTTLES = "Bay Area Shuttles"
    CALTRAIN = "Caltrain"
    COUNTY_CONNECTION = "County Connection"
    DUMBARTON = "Dumbarton"
    DUMBARTON_EXPRESS = "Dumbarton Express"
    EMERY_GO_ROUND = "Emery-Go-Round"
    EMERYVILLE_MTA = "Emeryville MTA"
    FAIRFIELD_SUISUN = "Fairfield-Suisun"
    FAST = "Fast"
    GOLDEN_GATE_FERRY = "Golden Gate Ferry"
    GOLDEN_GATE_TRANSIT = "Golden Gate Transit"
    GREYHOUND = "Greyhound"
    LAVTA = "LAVTA"
    MARIN_TRANSIT = "Marin Transit"
    MODESTO_TRANSIT = "Modesto Transit"
    MUNI = "Muni"
    NAPA_VINE = "Napa Vine"
    PETALUMA_TRANSIT = "Petaluma Transit"
    PRIVATE_SHUTTLE = "Private Shuttle"
    RIO_VISTA = "Rio-Vista"
    SAMTRANS = "SamTrans"
    SAN_JOAQUIN_TRANSIT = "San Joaquin Transit"
    SANTA_ROSA_CITY_BUS = "Santa Rosa City Bus"
    SF_BAY_FERRY = "SF Bay Ferry"
    SMART = "Smart"
    SOLTRANS = "SolTrans"
    SONOMA_COUNTY_TRANSIT = "Sonoma County Transit"
    STANFORD_SHUTTLES = "Stanford Shuttles"
    SAN_LEANDRO_LINKS = "San Leandro Links"
    TRI_DELTA = "Tri-Delta"
    UNION_CITY = "Union City"
    VACAVILLE_CITY_COACH = "Vacaville City Coach"
    VALLEJO_TRANSIT = "Vallejo Transit"
    VTA = "VTA"
    WESTCAT = "WestCat"
    WHEELS = "Wheels (LAVTA)"
    OPERATOR_OUTSIDE_BAY_AREA = "Operator Outside Bay Area"
    OTHER = "Other"


class FareMedium(str, Enum):
    """Payment method for fare."""

    CASH = "Cash"
    CLIPPER = "Clipper"
    CLIPPER_E_CASH = "Clipper (e-cash)"
    CLIPPER_PASS = "Clipper (pass)"
    CLIPPER_MONTHLY = "Clipper (monthly)"
    PASS = "Pass"
    MONTHLY_PASS = "Monthly Pass"
    DAY_PASS = "Day Pass"
    PAPER_TICKET = "Paper Ticket"
    MOBILE_APP = "Mobile App"
    E_TICKET = "E-Ticket"
    TRANSFER = "Transfer"
    FREE = "Free"
    OTHER = "Other"


class FareCategory(str, Enum):
    """Fare category/discount type."""

    ADULT = "Adult"
    YOUTH = "Youth"
    SENIOR = "Senior"
    DISABLED = "Disabled"
    STUDENT = "Student"
    MEDICARE = "Medicare"
    RTC = "RTC"
    FREE = "Free"
    OTHER = "Other"


class TripPurpose(str, Enum):
    """Purpose of trip origin or destination."""

    HOME = "Home"
    WORK = "Work"
    WORK_RELATED = "Work-related"
    SCHOOL = "School"
    COLLEGE = "College"
    UNIVERSITY = "University"
    HIGH_SCHOOL = "High school"
    GRADE_SCHOOL = "Grade school"
    SHOPPING = "Shopping"
    EAT_OUT = "Eat out"
    SOCIAL_RECREATION = "Social recreation"
    OTHER_MAINTENANCE = "Other maintenance"
    OTHER_DISCRETIONARY = "Other discretionary"
    ESCORTING = "Escorting"
    BUSINESS_APT = "Business apt"
    HOTEL = "Hotel"
    AIRPORT = "Airport"
    AT_WORK = "At work"
    OTHER = "Other"


class TechnologyType(str, Enum):
    """Transit vehicle technology type.
    
    Order matters - defines hierarchy for BEST_MODE (lowest to highest priority).
    """

    LOCAL_BUS = "Local Bus"
    EXPRESS_BUS = "Express Bus"
    FERRY = "Ferry Boat"
    LIGHT_RAIL = "Light Rail"
    HEAVY_RAIL = "Heavy Rail"
    COMMUTER_RAIL = "Commuter Rail"

    def to_short_code(self) -> str:
        """Convert to technology short code by taking first letter of each word.
        
        Returns:
            Short code string (e.g., "LB" for Local Bus, "FB" for Ferry Boat).
            
        Examples:
            >>> TechnologyType.LOCAL_BUS.to_short_code()
            'LB'
            >>> TechnologyType.FERRY.to_short_code()
            'FB'
        """
        # Take first letter of each word
        return "".join(word[0].upper() for word in self.value.split())

    def to_column_name(self) -> str:
        """Convert to database/column-safe name (lowercase with underscores).
        
        Returns:
            Column-safe name string (e.g., "local_bus", "ferry", "heavy_rail").
            Special case: "Ferry Boat" becomes "ferry" not "ferry_boat".
            
        Examples:
            >>> TechnologyType.LOCAL_BUS.to_column_name()
            'local_bus'
            >>> TechnologyType.FERRY.to_column_name()
            'ferry'
            >>> TechnologyType.HEAVY_RAIL.to_column_name()
            'heavy_rail'
        """
        # Special case for Ferry Boat -> ferry
        if self == TechnologyType.FERRY:
            return "ferry"
        # Convert to lowercase with underscores
        return self.value.replace(" ", "_").lower()

    @classmethod
    def hierarchy(cls) -> list["TechnologyType"]:
        """Get technology types in hierarchy order (lowest to highest priority).
        
        Returns list excluding MISSING.
        """
        return [
            cls.LOCAL_BUS,
            cls.EXPRESS_BUS,
            cls.FERRY,
            cls.LIGHT_RAIL,
            cls.HEAVY_RAIL,
            cls.COMMUTER_RAIL,
        ]

    def hierarchy_rank(self) -> int:
        """Get hierarchy rank (0=lowest priority, higher=more important)."""
        return self.hierarchy().index(self)


class Gender(str, Enum):
    """Gender of respondent."""

    MALE = "Male"
    FEMALE = "Female"
    NON_BINARY = "Non-binary"
    TRANSGENDER = "Transgender"
    OTHER = "Other"
    PREFER_NOT_TO_ANSWER = "Prefer not to answer"


# Hispanic/Latino ethnicity is now represented as bool | None (is_hispanic field)
# True = Hispanic/Latino or of Spanish origin
# False = Not Hispanic/Latino or of Spanish origin
# None = Unknown/missing


class Race(str, Enum):
    """Race/ethnicity category."""

    WHITE = "White"
    BLACK = "Black"
    ASIAN = "Asian"
    OTHER = "Other"


class WorkStatus(str, Enum):
    """Employment status."""

    FULL_OR_PART_TIME = "Full- or part-time"
    NON_WORKER = "Non-worker"
    OTHER = "Other"


class StudentStatus(str, Enum):
    """Student enrollment status."""

    FULL_OR_PART_TIME = "Full- or part-time"
    NON_STUDENT = "Non-student"


# NOTE: Standardize these values...
class EnglishProficiency(str, Enum):
    """Self-reported English proficiency."""

    VERY_WELL = "Very well"
    WELL = "Well"
    NOT_WELL = "Not well"
    LESS_THAN_WELL = "Less than well"
    NOT_WELL_AT_ALL = "Not well at all"
    NOT_AT_ALL = "Not at all"
    SKIP_PAPER_SURVEY = "Skip - Paper Survey"

# NOTE: Standardize these values...
class VehicleCount(str, Enum):
    """Number of vehicles in household."""

    ZERO = "Zero"
    ONE = "One"
    TWO = "Two"
    THREE = "Three"
    FOUR = "Four"
    FIVE = "Five"
    SIX = "Six"
    SEVEN = "Seven"
    EIGHT = "Eight"
    NINE = "Nine"
    TEN = "Ten"
    TWELVE = "Twelve"
    FOUR_OR_MORE = "Four or more"
    SIX_OR_MORE = "Six or more"
    DONT_KNOW = "Don't know"
    REF = "Ref"
    OTHER = "Other"

# NOTE: Standardize these values...
class HouseholdIncome(str, Enum):
    """Annual household income bracket."""

    UNDER_10K = "Under $10,000"
    UNDER_15K = "Under $15,000"
    UNDER_25K = "Under $25,000"
    UNDER_35K = "Under $35,000"
    UNDER_50K = "Under $50,000"
    FROM_10K_TO_25K = "$10,000 to $25,000"
    FROM_15K_TO_25K = "$15,000 to $25,000"
    FROM_15K_TO_30K = "$15,000 to $30,000"
    FROM_25K_TO_35K = "$25,000 to $35,000"
    FROM_25K_TO_50K = "$25,000 to $50,000"
    FROM_30K_TO_40K = "$30,000 to $40,000"
    FROM_35K_TO_50K = "$35,000 to $50,000"
    FROM_35K_OR_HIGHER = "$35,000 or higher"
    FROM_40K_TO_50K = "$40,000 to $50,000"
    FROM_50K_TO_60K = "$50,000 to $60,000"
    FROM_50K_TO_75K = "$50,000 to $75,000"
    FROM_50K_TO_100K = "$50,000 to $99,999"
    FROM_60K_TO_70K = "$60,000 to $70,000"
    FROM_70K_TO_80K = "$70,000 to $80,000"
    FROM_75K_TO_100K = "$75,000 to $100,000"
    FROM_80K_TO_100K = "$80,000 to $100,000"
    FROM_100K_TO_150K = "$100,000 to $149,999"
    FROM_100K_TO_150K_ALT = "$100,000 to $150,000"
    FROM_150K_OR_HIGHER = "$150,000 or higher"
    FROM_150K_TO_200K = "$150,000 to $200,000"
    FROM_200K_OR_HIGHER = "$200,000 or higher"
    REFUSED = "Refused"


class SurveyType(str, Enum):
    """Method of survey administration."""

    ONBOARD = "Onboard"
    PHONE = "Phone"
    ONLINE = "Online"
    TABLET = "Tablet"
    PAPER_MAIL_IN = "Paper - mail-in"
    PAPER_COLLECTED = "Paper - collected by interviewer"
    INTERVIEWER_ADMINISTERED = "Interviewer-administered"
    SELF_ADMINISTERED = "Self-administered"
    BRIEF_CATI = "Brief CATI"
    FULL_PAPER = "Full paper"
    TABLET_PI = "Tablet PI"
    ON_OFF_DUMMY = "On-off dummy"
    MIX = "Mix"
    OTHER = "Other"


class Language(str, Enum):
    """Language values used across survey fields.

    Used for interview language, field language, and language at home.
    When checking for OTHER sentinel value, use .str.to_titlecase()
    for case-insensitive comparison.
    """

    ENGLISH = "English"
    SPANISH = "Spanish"
    CHINESE = "Chinese"
    CANTONESE_CHINESE = "Cantonese Chinese"
    MANDARIN_CHINESE = "Mandarin Chinese"
    VIETNAMESE = "Vietnamese"
    TAGALOG = "Tagalog"
    KOREAN = "Korean"
    RUSSIAN = "Russian"
    FRENCH = "French"
    ARABIC = "Arabic"
    PUNJABI = "Punjabi"
    CANTONESE = "Cantonese"
    DONT_KNOW_REFUSE = "Don't know/refuse"
    SKIP_PAPER_SURVEY = "Skip - Paper Survey"
    OTHER = "Other"


# =============================================================================
# Pipeline-specific enums and mappings
# =============================================================================

class AutoRatioCategory(str, Enum):
    """Household vehicle-to-worker ratio categories."""

    ZERO_AUTOS = "Zero autos"
    WORKERS_GT_AUTOS = "Autos < workers"
    WORKERS_LE_AUTOS = "Autos >= workers"


class TourPurpose(str, Enum):
    """Tour purpose categories matching travel model definitions."""

    WORK = "Work"
    SCHOOL = "School"
    GRADE_SCHOOL = "Grade school"
    HIGH_SCHOOL = "High school"
    UNIVERSITY = "University"
    WORK_BASED = "Work-based"
    SHOPPING = "Shopping"
    OTHER_MAINTENANCE = "Other maintenance"
    EATING_OUT = "Eating out"
    OTHER_DISCRETIONARY = "Other discretionary"
    SOCIAL = "Social"
    ESCORTING = "Escorting"
    AT_WORK = "At-work"
