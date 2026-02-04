"""Typo corrections and value mappings for backfill operations.

This module contains field-specific dictionaries for fixing typos and
standardizing values to match database enums.
"""

import logging

import polars as pl

from .backfill_constants import ACCESS_EGRESS_MODE_FIXES, LANGUAGE_MAPPINGS

logger = logging.getLogger(__name__)

# ============================================================================
# TYPO FIXES & EXPLICIT VALUE MAPPINGS
# Applied after normalize_to_sentence_case to catch edge cases
# Organized by field category for maintainability
# ============================================================================

TYPO_FIXES = {
    # ------------------------------------------------------------------------
    # ACCESS & EGRESS MODES
    # ------------------------------------------------------------------------
    "access_mode": ACCESS_EGRESS_MODE_FIXES,
    "egress_mode": {**ACCESS_EGRESS_MODE_FIXES, ".": None},

    # ------------------------------------------------------------------------
    # SURVEY & FIELD METADATA
    # ------------------------------------------------------------------------
    "eng_proficient": {
        "Very Well": "Very well",
        "Unknown": None,
        "Skip - paper survey": "Skip - Paper Survey",
        "Missing": None,
    },
    "direction": {
        "Outboudn": "Outbound",
        "Oubound": "Outbound"
    },

    # ------------------------------------------------------------------------
    # FARE FIELDS (Largest section - fare_category and fare_medium)
    # ------------------------------------------------------------------------
    "fare_category": {
        "Easypass or class pass": "Other",
        "ADA": "Disabled",
        "Ada": "Disabled",
        "ADA ": "Disabled",
        " ADA": "Disabled",
        "ada": "Disabled",
        "Other Discount": "Other",
        "Senior Or Disabled": "Senior",
        "Senior or Disabled": "Senior",
        "Easypass Or Class Pass": "Other",
        "Easypass or Class Pass": "Other",
        "Rtc": "RTC",
        "Round Trip": "Adult",
        "Employee Discount": "Other",
        "Staff Discount-sjsu": "Other",
        "Ada/accomp": "Disabled",
        "Program Bus Pass Grom Turning Point": "Other",
        "City Pass": "Other",
        "Lynx Pass": "Other",
        "Lynxs": "Other",
        "Paper Ticket": "Adult",
        "One Way": "Adult",
        "Adult/student": "Adult",
        "Disabled/rtc": "RTC",
        "Youth(<19)": "Youth",
        "Do Not Know": "Missing",
        "Other/discounted Fare Category": "Other",
        "Promotional/special (not Specified)": "Other",
        "Student Discount": "Other",
        "Police Officer/leo": "Other",
        "Group Discount": "Other",
        "Senior 1/2 Off Promotion": "Senior",
        "Oakland A's Promotion": "Other",
        "Para": "Disabled",
        "Free Trip Card": "Other",
        "Machine Gives You Free Transfer, Bart Pays Cash": "Other",
        "Buy One/2nd Passenger Half-price/free Promotion": "Other",
    },

    "survey_type": {
        "Paper-collected by Interviewer": "Paper - collected by interviewer",
        "Paper-Collected by Interviewer": "Paper - collected by interviewer",
        "paper-collected by interviewer": "Paper - collected by interviewer",
        "Brief_cati": "Brief CATI",
        "Full_paper": "Full paper",
        "Tablet_pi": "Tablet PI",
        "On_off_dummy": "On-off dummy",
        "On off dummy": "On-off dummy",
        "Paper-mail-in": "Paper - mail-in",
        "Paper mail-in": "Paper - mail-in",
        "Interviewer-administered": "Interviewer-administered",
        "Interviewer - administered": "Interviewer-administered",
        "Self-administered": "Self-administered",
        "Self - administered": "Self-administered",
        "ONBOARD": "Onboard",
        "PHONE": "Phone",
        # work_status value appearing in survey_type - map to Missing
        "Full- or part-time": "Missing",
        # BART 2024 online survey
        "Online - modeling Platform": "Online"
    },

    "fare_medium": {
        # Transfer types - consolidate variations
        "Transfer (bart)":  "Transfer",
        "Transfer (Bart)": "Transfer",
        "Transfer (Bart Paper)": "Transfer",
        "Transfer (bart Paper)": "Transfer",
        "Transfer (paper with Mag Strip)": "Transfer",
        "Transfer (ace)": "Transfer",
        "Transfer (golden Gate or Ferry Shutt": "Transfer",
        "Transfer (county Connection Paper)": "Transfer",
        "Transfer (wheels)": "Transfer",
        "Transfer (bus)": "Transfer",
        "Transfer (santa Rosa Citybus)": "Transfer",
        "Transfer (ac/dex)": "Transfer",
        "Transfer (not Santa Rosa Citybus)": "Transfer",
        "Capitol Corridor Transfer": "Transfer",

        # Mobile/online - fix capitalization
        "Amtrak Mobile App": "Mobile App",
        "Mobile app": "Mobile App",
        "Website (capitol Corridor or Amtrak)": "E-Ticket",
        "Website": "E-Ticket",
        "Conductor/on Train": "Paper Ticket",

        # Cash variations - fix capitalization
        "Clipper Cash": "Clipper (e-cash)",
        "Cash (coins and Bills)": "Cash",
        "Cash (round Trip)": "Cash",
        "Cash (single Ride)": "Cash",
        "Cash (one Way)": "Cash",
        "Cash or Paper": "Cash",
        "Paper or Cash": "Cash",
        "Token": "Cash",
        "Free Ride Token": "Free",
        "Exempt (employee, Law Enforcement)": "Free",
        "Paratransit Free Rider": "Free",
        "Courtesy Fare": "Free",
        "No Payment": "Free",
        "Did Not Pay": "Free",

        # Disability-related fare types
        "Disabled": "Pass",

        # Clipper variations
        "Clipper Pass": "Clipper (pass)",
        "Clipper Start": "Clipper (pass)",
        "Clipper 31-day Pass": "Clipper (monthly)",
        "Clipper Day Pass": "Clipper (pass)",

        # Pass types - fix capitalization
        "Gold Card": "Pass",
        "Employer/wage Works": "Pass",
        "Employer/university/group pass": "Pass",
        "Employer/university/group Pass": "Pass",
        "Srjc Student Pass": "Pass",
        "College of Marin Pass": "Pass",
        "College": "Pass",
        "Youth Quarterly Pass": "Pass",
        "Youth Unlimited Pass": "Pass",
        "Way2go Pass": "Pass",
        "Veteran Pass": "Pass",
        "Rtc Discount Card": "Pass",
        "Discount (rtc)": "Pass",
        "10-ride Pass": "Pass",
        "20-ride Pass": "Pass",
        "20 Ride": "Pass",
        "Punch-20": "Pass",
        "Paper Pass": "Pass",
        "Low Income Pass": "Pass",
        "Free Youth/student Pass": "Pass",
        "Hopthru Pass": "Pass",
        "Ecopass": "Pass",
        "Change Card": "Pass",
        "Superpass": "Pass",
        "Parking Pass Hercules": "Pass",
        "Parking Pass Hercules Transit": "Pass",

        # Monthly pass variations - fix capitalization
        "Monthly": "Monthly Pass",
        "Monthly pass": "Monthly Pass",
        "31-day Pass": "Monthly Pass",
        "Adult 31-day Pass": "Monthly Pass",
        "Half Price 31-day Pass": "Monthly Pass",
        "Half Price Monthly": "Monthly Pass",
        "Youth 31-day Pass": "Monthly Pass",
        "Adult 31-day": "Monthly Pass",
        "Adult Monthly": "Monthly Pass",
        "Youth Monthly": "Monthly Pass",
        "Cash 31-day Pass": "Monthly Pass",
        "31 Day Pass Lynx": "Monthly Pass",
        "Lynx 31 Day": "Monthly Pass",
        "Lynx 31 Day Pass": "Monthly Pass",
        "31 Day Lynx": "Monthly Pass",
        "Monthly Intercity": "Monthly Pass",

        # Day pass variations - fix capitalization
        "Cash 24-hour Pass": "Day Pass",
        "One Day Pass": "Day Pass",
        "Adult Day Pass": "Day Pass",
        "Youth Day Pass": "Day Pass",
        "Half Price Day Pass": "Day Pass",
        "20 Day Pass": "Pass",  # Not monthly, not day - general pass
        "Bart Transfer For $1.50": "Transfer",

        # Other fare media
        "From a Station Agent": "Other",
        "Local City or Transit Office (city of Roseville, Placer County Transit, Etc.)": (
            "Other"
        ),
        "1-way Rv/isleton Dev": "Other",
        "Do Not Know": "Missing",

        # Final batch of specific fare media from validation errors
        "Transfer From Bart Free": "Transfer",
        "Ac Transit Transfer": "Transfer",
        "Transfer (county Connection)": "Transfer",
        "Caltrain (2+ Zones)": "Paper Ticket",
        "Westcat Parking Pass Trip": "Pass",
        "Clipper 10-ride Pass": "Clipper (pass)",
        "Senior Pass Buy at Lake Merriot Station": "Pass",
        "12 Day Pass": "Day Pass",
        "20 Ride Pass": "Pass",
        "The Superpass": "Pass",
        "Medicare": "Pass",
        "Fact Bus Buss Program": "Pass",
        "County Provided": "Pass",
        "Gvnt Bus Pass": "Pass",
        "Bus Pass Provided by Work": "Pass",
        "Senior": "Pass",
        "Free City Bus Employye": "Free",
        "County Employee": "Free",
        "Ada": "Free",
        "Ambassador Pass": "Pass",

        # RTC and other program-specific fare media
        "Rediwheels Rtc": "Pass",
        "Redi-Wheels Rtc": "Pass",
        "Rtc": "Pass"
    },

    # ------------------------------------------------------------------------
    # DEMOGRAPHICS & HOUSEHOLD
    # ------------------------------------------------------------------------
    "household_income_category": {
        "Refused": "Missing",
        "refused": "Missing",
        "$200,000 or Higher": "$200,000 or higher",
        "$150,000 or Higher": "$150,000 or higher",
        "$35,000 or Higher": "$35,000 or higher",
        "under $10,000": "Under $10,000",
        "under $35,000": "Under $35,000"
    },
    "work_status": {
        "Full- Or Part-Time": "Full- or part-time",
        "Non-Worker": "Non-worker"
    },
    "student_status": {
        "Full- Or Part-Time": "Full- or part-time",
        "Non-Student": "Non-student",
        "Missing": None
    },
    "gender": {
        "Non-Binary": "Non-binary",
        "Prefer Not To Answer": "Prefer not to answer",
        "No Answer": "Prefer not to answer",
        "female": "Female",
        "male": "Male"
    },
    "race": {
        "WHITE": "White",
        "ASIAN": "Asian",
        "BLACK": "Black",
        "OTHER": "Other",
        "Missing": None
    },

    # ------------------------------------------------------------------------
    # LANGUAGE FIELDS
    # ------------------------------------------------------------------------
    "field_language": LANGUAGE_MAPPINGS,
    "interview_language": {
        **LANGUAGE_MAPPINGS,
        "Cantonese Chinese": "Cantonese Chinese",  # Keep as-is (matches enum)
        "Mandarin Chinese": "Mandarin Chinese"      # Keep as-is (matches enum)
    },

    # ------------------------------------------------------------------------
    # TRANSFER OPERATORS
    # ------------------------------------------------------------------------
    "transfer_from": {
        "Santa Rosa CityBus": "Santa Rosa City Bus",
        "Emeryville Mta": "Emeryville MTA",
        "Wheels (lavta)": "Wheels (LAVTA)",
        "Emery-go-round": "Emery-Go-Round",
        "Rio-vista": "Rio-Vista",
        "Fairfield-suisun": "Fairfield-Suisun",
        "Blue & Gold Ferry": "Blue Gold Ferry"
    },

    "transfer_to": {
        "Santa Rosa CityBus": "Santa Rosa City Bus",
        "Santa Rosa Citybus": "Santa Rosa City Bus",
        "Emeryville Mta": "Emeryville MTA",
        "Wheels (lavta)": "Wheels (LAVTA)",
        "Emery-go-round": "Emery-Go-Round",
        "Blue & Gold Ferry": "Blue Gold Ferry",
        "Fairfield-suisun": "Fairfield-Suisun"
    },
    "day_part": {
        "NIGHT": "EVENING",  # Normalize NIGHT to EVENING for DayPart enum
        "WEEKEND": None,  # WEEKEND not a valid time of day, map to None
        ".": None,  # Period indicates missing data
        "Missing": None  # Explicit missing values
    }
}


def apply_typo_fixes(df: pl.DataFrame) -> pl.DataFrame:
    """Apply typo fixes and value mappings to DataFrame."""
    for field, mapping in TYPO_FIXES.items():
        if field in df.columns:
            df = df.with_columns(
                pl.col(field).replace_strict(mapping, default=pl.col(field)).alias(field)
            )
    return df


def global_enum_cleanup(df: pl.DataFrame) -> pl.DataFrame:
    """Replace any remaining 'Missing' strings with None for enum fields."""
    enum_fields = [
        "direction", "access_mode", "egress_mode", "first_board_tech", "last_alight_tech",
        "orig_purp", "dest_purp", "fare_category", "day_of_the_week", "race",
        "work_status", "student_status", "interview_language", "field_language",
        "transfer_1_operator", "transfer_2_operator", "transfer_3_operator",
        "fare_medium", "household_income_category", "survey_type", "weekpart",
        "day_part", "eng_proficient", "tour_purp", "trip_purp", "gender"
    ]
    df = df.with_columns([
        pl.when(pl.col(field) == "Missing").then(None).otherwise(pl.col(field)).alias(field)
        for field in enum_fields if field in df.columns
    ])
    return df
