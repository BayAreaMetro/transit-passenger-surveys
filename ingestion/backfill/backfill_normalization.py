"""Text normalization functions for backfill operations.

This module contains functions for normalizing text fields to match
codebook standards, including sentence case conversion and special patterns.
"""

import logging

import polars as pl

from .backfill_constants import (
    ACRONYM_MAX_LENGTH,
    ACRONYM_MIN_LENGTH,
    EXACT_MAPPINGS,
    KNOWN_ACRONYMS,
    LOWERCASE_WORDS,
)

logger = logging.getLogger(__name__)


def check_special_patterns(text: str) -> str | None:
    """Check for special text patterns that need specific handling.

    Returns None if no match.
    """
    text_lower = text.lower()

    # Full-time/part-time patterns
    if text_lower.startswith(("full-", "full ")):
        return "Full- or part-time"

    # Non- prefix patterns
    if text_lower.startswith("non-"):
        if "binary" in text_lower:
            return "Non-binary"
        if "student" in text_lower:
            return "Non-student"
        if "worker" in text_lower:
            return "Non-worker"

    return None


def simplify_fare_medium(text: str) -> str | None:
    """Simplify complex fare medium values.

    Returns None if not a fare medium pattern.
    """
    text_lower = text.lower()
    result: str | None = None

    # Clipper variants
    if "clipper" in text_lower and "(" in text:
        if "e-cash" in text_lower or "ecash" in text_lower:
            result = "Clipper (e-cash)"
        elif "monthly" in text_lower:
            result = "Clipper (monthly)"
        else:
            # Default for ride variants and others
            result = "Clipper (pass)"

    # Ticket variants - all map to Paper Ticket
    elif "ticket" in text_lower:
        result = "Paper Ticket"

    # Pass variants
    elif "pass" in text_lower and "(" in text:
        if "day" in text_lower or "24" in text:
            result = "Day Pass"
        elif "monthly" in text_lower or "31" in text:
            result = "Monthly Pass"
        else:
            result = "Pass"

    return result


def _process_word(word: str, position: int, previous_word: str) -> str:
    """Process a single word for sentence case conversion.

    Args:
        word: The word to process
        position: Word position in the text (0-indexed)
        previous_word: The previous word in the sequence
    """
    # Keep all-caps acronyms
    if word.isupper() and ACRONYM_MIN_LENGTH <= len(word) <= ACRONYM_MAX_LENGTH:
        return word if word in KNOWN_ACRONYMS else word.capitalize()

    # Lowercase specific words when not first, or words after hyphens
    if (position > 0 and word.lower() in LOWERCASE_WORDS) or (
        position > 0 and previous_word and previous_word.endswith("-")
    ):
        return word.lower()

    # Handle compound words with hyphens at start
    if "-" in word and position == 0:
        parts = word.split("-")
        return parts[0].capitalize() + "-" + "-".join(p.lower() for p in parts[1:])

    # First word - capitalize
    if position == 0:
        return word.capitalize()

    # Keep mixed case words that look like proper nouns
    if word[0].isupper() and not word.isupper() and len(word) > 1:
        return word

    return word.capitalize()


def normalize_to_sentence_case(text: str) -> str:
    """Convert text to proper sentence case matching codebook standards."""
    if not text or text == ".":
        return text

    # Strip whitespace and replace underscores
    text = text.strip().replace("_", " ")

    # Exact match mappings (case-insensitive input)
    text_upper = text.upper()
    for key, value in EXACT_MAPPINGS.items():
        if text_upper == key.upper():
            return value

    # Check special patterns
    special_result = check_special_patterns(text)
    if special_result:
        return special_result

    # Check fare medium simplifications
    fare_result = simplify_fare_medium(text)
    if fare_result:
        return fare_result

    # Convert to title case
    words = text.split()
    result = []

    for i, word in enumerate(words):
        previous = result[-1] if result else ""
        result.append(_process_word(word, i, previous))

    return " ".join(result)


def apply_normalization(df: pl.DataFrame) -> pl.DataFrame:
    """Apply text normalization to all categorical fields.

    Normalizes complex categorical fields efficiently by mapping unique values.
    This avoids slow row-by-row Python UDF calls.
    """
    # Complex categorical fields that need normalization
    categorical_fields_complex = [
        "work_status", "student_status", "household_income_category",
        "survey_type", "interview_language", "field_language",
        "orig_purp", "dest_purp", "tour_purp", "trip_purp", "transfer_from", "transfer_to",
        "access_mode", "egress_mode", "fare_medium", "eng_proficient", "fare_category"
    ]

    for field in categorical_fields_complex:
        if field not in df.columns:
            continue
        unique_vals = df[field].unique().drop_nulls().to_list()
        if unique_vals:
            mapping = {val: normalize_to_sentence_case(val) for val in unique_vals}
            df = df.with_columns(
                pl.col(field).replace_strict(mapping, default=pl.col(field)).alias(field)
            )

    # Simple title case for fields without complex rules
    categorical_fields_simple = [
        "weekpart", "day_of_the_week", "direction",
        "vehicle_tech", "first_board_tech", "last_alight_tech", "gender", "race"
    ]
    df = df.with_columns([
        pl.col(field).str.to_titlecase().alias(field)
        for field in categorical_fields_simple if field in df.columns
    ])

    # Fix Ferry→Ferry Boat for technology fields
    for tech_field in ["vehicle_tech", "first_board_tech", "last_alight_tech"]:
        if tech_field in df.columns:
            df = df.with_columns(
                pl.when(pl.col(tech_field) == "Ferry")
                .then(pl.lit("Ferry Boat"))
                .otherwise(pl.col(tech_field))
                .alias(tech_field)
            )

    return df
