"""Constants and mapping dictionaries for backfill operations.

This module contains all shared constants, lookup dictionaries, and mappings
used during the backfill process to normalize and standardize survey data.
"""

from pathlib import Path

# ============================================================================
# CONFIGURATION CONSTANTS
# ============================================================================

# Date format lengths
SHORT_DATE_LENGTH = 8  # MM-DD-YY format
ACRONYM_MIN_LENGTH = 2  # Minimum length for acronym detection
ACRONYM_MAX_LENGTH = 4  # Maximum length for acronym detection
MAX_SAMPLE_LOCATIONS = 3  # Number of sample locations to show in error messages

# ============================================================================
# DATA PATHS
# ============================================================================

# Path to standardized data
STANDARDIZED_DATA_PATH = Path(
    r"\\models.ad.mtc.ca.gov\data\models\Data\OnBoard\Data and Reports\_data_Standardized"
)

# ============================================================================
# WORD TO NUMBER MAPPINGS
# ============================================================================

# Word-to-number conversion for numeric fields (persons, workers, vehicles)
WORD_TO_NUMBER = {
    "zero": 0, "one": 1, "two": 2, "three": 3, "four": 4, "five": 5,
    "six": 6, "seven": 7, "eight": 8, "nine": 9, "ten": 10,
    "eleven": 11, "twelve": 12, "thirteen": 13, "fourteen": 14, "fifteen": 15,
    "sixteen": 16, "seventeen": 17, "eighteen": 18, "nineteen": 19, "twenty": 20,
    "twenty-one": 21, "twenty-two": 22, "twenty-three": 23, "twenty-four": 24, "twenty-five": 25,
    "twenty-six": 26, "twenty-seven": 27, "twenty-eight": 28, "twenty-nine": 29, "thirty": 30,
    "none": 0, "other": None, "DON'T KNOW": None, "Ref": None, "missing": None,
    "four or more": 4, "five or more": 5, "six or more": 6, "ten or more": 10
}

# ============================================================================
# LANGUAGE MAPPINGS
# ============================================================================

# Language normalization (ALL CAPS → Title Case) - shared by field_language and interview_language
LANGUAGE_MAPPINGS = {
    "CHINESE": "Chinese", "ENGLISH": "English", "SPANISH": "Spanish", "OTHER": "Other",
    "FRENCH": "French", "KOREAN": "Korean", "RUSSIAN": "Russian", "ARABIC": "Arabic",
    "PUNJABI": "Punjabi", "CANTONESE": "Cantonese", "TAGALOG": "Tagalog",
    "TAGALOG/FILIPINO": "Tagalog", "Tagalog/Filipino": "Tagalog",
    "VIETNAMESE": "Vietnamese", "DONT KNOW/REFUSE": None,
    "Skip - Paper Survey": "Skip - Paper Survey", "Skip - paper survey": "Skip - Paper Survey"
}

# ============================================================================
# ACCESS/EGRESS MODE FIXES
# ============================================================================

# Access/Egress mode fixes - shared by both fields
ACCESS_EGRESS_MODE_FIXES = {
    "Tnc": "TNC",
    "Missing - dummy record": None,
    "Missing - dummy Record": None,
    "Unknown": "Other",
    "Missing - question not asked": None,
    "Missing - question Not Asked": None,
}

# ============================================================================
# TEXT NORMALIZATION MAPPINGS
# ============================================================================

# Known acronyms that should stay uppercase
KNOWN_ACRONYMS = {
    "AC", "BART", "VTA", "ACE", "SMART", "SF", "TAP", "TAZ",
    "MAZ", "ID", "CATI", "PI", "AM", "PM", "RTC", "PNR", "KNR", "TNC"
}

# Words that should be lowercase (except when first word)
LOWERCASE_WORDS = {
    "or", "of", "and", "the", "a", "an", "in", "on", "at", "to", "by", "with"
}

# Exact match mappings for text normalization (case-insensitive input)
EXACT_MAPPINGS = {
    "PREFER NOT TO ANSWER": "Prefer not to answer",
    "MUNI": "Muni", "AC TRANSIT": "AC Transit", "SAMTRANS": "SamTrans",
    "WESTCAT": "WestCat", "SOLTRANS": "SolTrans", "TRI-DELTA": "Tri-Delta",
    "LAVTA": "LAVTA", "PNR": "PNR", "KNR": "KNR",
    "NOT WELL": "Not well", "NOT AT ALL": "Not at all",
    "LESS THAN WELL": "Less than well", "NOT WELL AT ALL": "Not well at all",
    "VERY WELL": "Very well",
    "TABLET_PI": "Tablet PI", "TABLET PI": "Tablet PI",
    "BRIEF_CATI": "Brief CATI", "BRIEF CATI": "Brief CATI",
    "OTHER MAINTENANCE": "Other maintenance", "OTHER DISCRETIONARY": "Other discretionary",
    "SOCIAL RECREATION": "Social recreation", "WORK-RELATED": "Work-related",
    "BUSINESS APT": "Business apt", "EAT OUT": "Eat out",
    "HIGH SCHOOL": "High school", "GRADE SCHOOL": "Grade school", "AT WORK": "At work",
    "CASH (BILLS AND COINS)": "Cash", "TRANSFER (BART PAPER + CASH)": "Transfer",
    "FOUR OR MORE": "Four or more", "SIX OR MORE": "Six or more",
    "DON'T KNOW": "Don't know", "HOTELS": "Hotel",
    "ON OFF DUMMY": "On-off dummy",
    "INTERVIEWER - ADMINISTERED": "Interviewer-administered",
    "SELF - ADMINISTERED": "Self-administered",
    "SKIP - PAPER SURVEY": "Skip - Paper Survey",
    "TAGALOG/FILIPINO": "Tagalog",
    "SANTA ROSA CITYBUS": "Santa Rosa City Bus", "EMERYVILLE MTA": "Emeryville MTA",
    "WHEELS (LAVTA)": "Wheels (LAVTA)", "EMERY-GO-ROUND": "Emery-Go-Round",
    "RIO-VISTA": "Rio-Vista", "FAIRFIELD-SUISUN": "Fairfield-Suisun",
    "BLUE & GOLD FERRY": "Blue Gold Ferry",
}
