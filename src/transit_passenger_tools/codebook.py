"""
Enumerated types for transit passenger survey data.

This module defines all the categorical value types used across surveys.
Each enum uses LabeledEnum to provide integer values and descriptive labels.
"""

from data_canon.core.labeled_enum import LabeledEnum


class AccessMode(LabeledEnum):
    """Mode used to access transit from origin."""

    canonical_field_name = "access_mode"
    field_description = "Mode used to access transit from trip origin"

    WALK = (1, "Walk")
    BIKE = (2, "Bike")
    PNR = (3, "Park and Ride")
    KNR = (4, "Kiss and Ride")
    TNC = (5, "Transportation Network Company")
    OTHER = (6, "Other")


class EgressMode(LabeledEnum):
    """Mode used to egress from transit to destination."""

    canonical_field_name = "egress_mode"
    field_description = "Mode used to egress from transit to trip destination"

    WALK = (1, "Walk")
    BIKE = (2, "Bike")
    PNR = (3, "Park and Ride")
    KNR = (4, "Kiss and Ride")
    TNC = (5, "Transportation Network Company")
    OTHER = (6, "Other")


class TripPurpose(LabeledEnum):
    """Purpose of trip origin or destination."""

    canonical_field_name = "trip_purpose"
    field_description = "Purpose at trip origin or destination"

    HOME = (1, "Home")
    WORK = (2, "Work")
    WORK_RELATED = (3, "Work-related")
    COLLEGE = (4, "College")
    SCHOOL = (5, "School (K-12)")
    GRADE_SCHOOL = (6, "Grade School")
    SHOPPING = (7, "Shopping")
    EAT_OUT = (8, "Eat Out")
    SOCIAL_RECREATION = (9, "Social/Recreation")
    OTHER_MAINTENANCE = (10, "Other Maintenance")
    OTHER_DISCRETIONARY = (11, "Other Discretionary")
    ESCORTING = (12, "Escorting")
    BUSINESS_APT = (13, "Business Appointment")


class Gender(LabeledEnum):
    """Gender of respondent."""

    canonical_field_name = "gender"
    field_description = "Gender identity of survey respondent"

    MALE = (1, "Male")
    FEMALE = (2, "Female")
    OTHER = (3, "Other")
    MISSING = (99, "Missing/Refused")


class HouseholdIncome(LabeledEnum):
    """Annual household income bracket."""

    canonical_field_name = "household_income"
    field_description = "Annual household income bracket"

    UNDER_10K = (1, "Under $10,000")
    FROM_10K_TO_25K = (2, "$10,000 to $24,999")
    FROM_25K_TO_35K = (3, "$25,000 to $34,999")
    FROM_35K_TO_50K = (4, "$35,000 to $49,999")
    FROM_50K_TO_75K = (5, "$50,000 to $74,999")
    FROM_75K_TO_100K = (6, "$75,000 to $99,999")
    FROM_100K_TO_150K = (7, "$100,000 to $149,999")
    OVER_150K = (8, "$150,000 or more")
    REFUSED = (98, "Refused")
    MISSING = (99, "Missing")


class WorkStatus(LabeledEnum):
    """Employment status."""

    canonical_field_name = "work_status"
    field_description = "Employment status of respondent"

    FULL_OR_PART_TIME = (1, "Employed (Full or Part Time)")
    NON_WORKER = (2, "Not Employed")
    MISSING = (99, "Missing/Refused")


class StudentStatus(LabeledEnum):
    """Student enrollment status."""

    canonical_field_name = "student_status"
    field_description = "Student enrollment status of respondent"

    FULL_OR_PART_TIME = (1, "Student (Full or Part Time)")
    NON_STUDENT = (2, "Not a Student")
    MISSING = (99, "Missing/Refused")


class FareCategory(LabeledEnum):
    """Fare category/discount type."""

    canonical_field_name = "fare_category"
    field_description = "Fare category or discount type used"

    ADULT = (1, "Adult")
    YOUTH = (2, "Youth")
    SENIOR = (3, "Senior")
    DISABLED = (4, "Disabled")
    MISSING = (99, "Missing")


class FareMedium(LabeledEnum):
    """Payment method for fare."""

    canonical_field_name = "fare_medium"
    field_description = "Payment medium used for fare"

    CASH = (1, "Cash")
    CLIPPER = (2, "Clipper")
    OTHER = (3, "Other")
    MISSING = (99, "Missing")


class ClipperDetail(LabeledEnum):
    """Specific Clipper card payment type."""

    canonical_field_name = "clipper_detail"
    field_description = "Specific Clipper card payment type"

    E_CASH = (1, "Clipper E-Cash")
    PASS = (2, "Clipper Pass")
    MISSING = (99, "Missing")


class SurveyType(LabeledEnum):
    """Method of survey administration."""

    canonical_field_name = "survey_type"
    field_description = "Method of survey administration"

    BRIEF_CATI = (1, "Brief CATI (Phone)")
    FULL_PAPER = (2, "Full Paper Survey")
    TABLET_PI = (3, "Tablet/Online with Personal Interview")


class Weekpart(LabeledEnum):
    """Day type when survey was conducted."""

    canonical_field_name = "weekpart"
    field_description = "Day type when survey was conducted"

    WEEKDAY = (1, "Weekday")
    WEEKEND = (2, "Weekend")


class Direction(LabeledEnum):
    """Direction of travel."""

    canonical_field_name = "direction"
    field_description = "Direction of transit travel"

    NORTHBOUND = (1, "Northbound")
    SOUTHBOUND = (2, "Southbound")
    EASTBOUND = (3, "Eastbound")
    WESTBOUND = (4, "Westbound")


class InterviewLanguage(LabeledEnum):
    """Language used for survey interview."""

    canonical_field_name = "interview_language"
    field_description = "Language in which survey interview was conducted"

    ENGLISH = (1, "English")
    SPANISH = (2, "Spanish")
    CHINESE = (3, "Chinese")
    VIETNAMESE = (4, "Vietnamese")
    TAGALOG = (5, "Tagalog")
    KOREAN = (6, "Korean")
    RUSSIAN = (7, "Russian")
    OTHER = (99, "Other")


class LanguageAtHomeBinary(LabeledEnum):
    """Whether language other than English is spoken at home."""

    canonical_field_name = "language_at_home_binary"
    field_description = "Whether language other than English is spoken at home"

    ENGLISH_ONLY = (1, "English Only")
    OTHER = (2, "Other Language")


class EnglishProficiency(LabeledEnum):
    """Self-reported English proficiency."""

    canonical_field_name = "eng_proficient"
    field_description = "Self-reported English language proficiency"

    VERY_WELL = (1, "Very Well")
    WELL = (2, "Well")
    NOT_WELL = (3, "Not Well")
    NOT_AT_ALL = (4, "Not at All")
    MISSING = (99, "Missing")


class Hispanic(LabeledEnum):
    """Hispanic/Latino ethnicity."""

    canonical_field_name = "hispanic"
    field_description = "Hispanic or Latino ethnicity"

    HISPANIC = (1, "Hispanic/Latino or of Spanish Origin")
    NOT_HISPANIC = (2, "Not Hispanic/Latino or of Spanish Origin")
    MISSING = (99, "Missing")


class HouseholdSize(LabeledEnum):
    """Number of persons in household."""

    canonical_field_name = "persons"
    field_description = "Number of persons in household"

    ONE = (1, "One")
    TWO = (2, "Two")
    THREE = (3, "Three")
    FOUR = (4, "Four")
    FIVE = (5, "Five")
    SIX = (6, "Six")
    SEVEN = (7, "Seven")
    EIGHT = (8, "Eight")
    NINE = (9, "Nine")
    TEN_OR_MORE = (10, "Ten or More")


class VehicleCount(LabeledEnum):
    """Number of vehicles in household."""

    canonical_field_name = "vehicles"
    field_description = "Number of vehicles in household"

    ZERO = (0, "Zero")
    ONE = (1, "One")
    TWO = (2, "Two")
    THREE = (3, "Three")
    FOUR_OR_MORE = (4, "Four or More")


class WorkerCount(LabeledEnum):
    """Number of workers in household."""

    canonical_field_name = "workers"
    field_description = "Number of workers in household"

    ZERO = (0, "Zero")
    ONE = (1, "One")
    TWO = (2, "Two")
    THREE = (3, "Three")
    FOUR = (4, "Four")
    FIVE = (5, "Five")
    SIX_OR_MORE = (6, "Six or More")


class TransferCount(LabeledEnum):
    """Number of transfers made."""

    canonical_field_name = "number_transfers"
    field_description = "Number of transfers made"

    ZERO = (0, "Zero")
    ONE = (1, "One")
    TWO = (2, "Two")
    THREE = (3, "Three")
    FOUR_OR_MORE = (4, "Four or More")

