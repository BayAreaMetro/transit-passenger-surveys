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
    MISSING = "Missing"
    MISSING_DUMMY = "Missing - Dummy Record"
    MISSING_NOT_ASKED = "Missing - Question Not Asked"


# NOTE: Standardize these values... maybe...
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
    MISSING = "Missing"


class DayOfWeek(str, Enum):
    """Day of the week."""

    MONDAY = "Monday"
    TUESDAY = "Tuesday"
    WEDNESDAY = "Wednesday"
    THURSDAY = "Thursday"
    FRIDAY = "Friday"
    SATURDAY = "Saturday"
    SUNDAY = "Sunday"
    MISSING = "Missing"


class Weekpart(str, Enum):
    """Day type when survey was conducted."""

    WEEKDAY = "Weekday"
    WEEKEND = "Weekend"
    MISSING = "Missing"


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
    MISSING = "Missing"


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
    MISSING = "Missing"


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
    MISSING = "Missing"


class TripPurpose(str, Enum):
    """Purpose of trip origin or destination."""

    HOME = "Home"
    WORK = "Work"
    WORK_RELATED = "Work-related"
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
    MISSING = "Missing"


class TechnologyType(str, Enum):
    """Transit vehicle technology type."""

    LOCAL_BUS = "Local Bus"
    EXPRESS_BUS = "Express Bus"
    LIGHT_RAIL = "Light Rail"
    HEAVY_RAIL = "Heavy Rail"
    COMMUTER_RAIL = "Commuter Rail"
    FERRY = "Ferry"
    MISSING = "Missing"


class Gender(str, Enum):
    """Gender of respondent."""

    MALE = "Male"
    FEMALE = "Female"
    NON_BINARY = "Non-binary"
    TRANSGENDER = "Transgender"
    OTHER = "Other"
    PREFER_NOT_TO_ANSWER = "Prefer not to answer"
    MISSING = "Missing"


class Hispanic(str, Enum):
    """Hispanic/Latino ethnicity."""

    HISPANIC = "Hispanic/Latino or of Spanish origin"
    NOT_HISPANIC = "Not Hispanic/Latino or of Spanish origin"
    MISSING = "Missing"


class Race(str, Enum):
    """Race/ethnicity category."""

    WHITE = "White"
    BLACK = "Black"
    ASIAN = "Asian"
    OTHER = "Other"
    MISSING = "Missing"


class WorkStatus(str, Enum):
    """Employment status."""

    FULL_OR_PART_TIME = "Full- or part-time"
    NON_WORKER = "Non-worker"
    OTHER = "Other"
    MISSING = "Missing"


class StudentStatus(str, Enum):
    """Student enrollment status."""

    FULL_OR_PART_TIME = "Full- or part-time"
    NON_STUDENT = "Non-student"
    MISSING = "Missing"

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
    MISSING = "Missing"

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
    MISSING = "Missing"

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
    MISSING = "Missing"


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
    MISSING = "Missing"


class InterviewLanguage(str, Enum):
    """Language used for survey interview."""

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
    MISSING = "Missing"


class FieldLanguage(str, Enum):
    """Survey field language."""

    ENGLISH = "English"
    SPANISH = "Spanish"
    CHINESE = "Chinese"
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
    MISSING = "Missing"
