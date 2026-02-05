"""Strict validation for preprocessed survey data.

This module enforces the contract between preprocessing scripts and the pipeline.

ARCHITECTURE:
- Preprocessing scripts (one-off, vendor-specific) transform data → CoreSurveyResponse format
- CoreSurveyResponse schema IS the contract - all required fields must be present
- Extra fields not in schema are fine (vendor-specific, ignored by pipeline)
- Pipeline validates core fields exist and performs final derivations
- Validation is 100% model-driven from data models

Model Structure:
- CoreSurveyResponse: Vendor-provided fields (inputs to pipeline)
- DerivedSurveyResponse: Pipeline-calculated fields (inherits Core)
- SurveyResponse: Complete schema (inherits Derived)

For categorical fields, MISSING enum values distinguish "not asked" from processing errors.
"""

import logging
import types
from enum import Enum
from typing import Union, get_args, get_origin

import polars as pl
from pydantic.fields import FieldInfo

from transit_passenger_tools.pipeline import (
    auto_sufficiency,
    date_time,
    demographics,
    geocoding,
    path_labels,
    tour_purpose,
    transfers,
)
from transit_passenger_tools.schemas.models import CoreSurveyResponse

logger = logging.getLogger(__name__)


def get_field_dependencies() -> dict[str, list[str]]:
    """Get field dependencies from all pipeline modules.

    Returns:
        Dictionary mapping each output field to its required input fields.
        Example: {"tour_purp": ["orig_purp", "dest_purp", "work_status"], ...}
    """
    # Import pipeline modules dynamically to read their FIELD_DEPENDENCIES
    modules = [
        auto_sufficiency,
        date_time,
        demographics,
        geocoding,
        path_labels,
        tour_purpose,
        transfers,
    ]

    dependencies = {}

    for module in modules:
        if not hasattr(module, "FIELD_DEPENDENCIES"):
            logger.warning("Module %s missing FIELD_DEPENDENCIES", module.__name__)
            continue

        if hasattr(module, "FIELD_DEPENDENCIES"):
            field_deps = module.FIELD_DEPENDENCIES
            input_fields = field_deps.inputs
            output_fields = field_deps.outputs

            # Map each output field to the module's input fields
            for output_field in output_fields:
                dependencies[output_field] = input_fields

    return dependencies


def extract_core_fields() -> list[str]:
    """Extract all field names from CoreSurveyResponse.

    Returns:
        List of core field names
    """
    return list(CoreSurveyResponse.model_fields.keys())


def extract_enum_validators() -> dict[str, type[Enum]]:
    """Extract enum validators from CoreSurveyResponse field type hints.

    Returns:
        Dictionary mapping field name to Enum class
    """
    validators = {}

    for field_name, field_info in CoreSurveyResponse.model_fields.items():
        if not isinstance(field_info, FieldInfo):
            continue

        # Get the field's annotation (type hint)
        annotation = field_info.annotation

        # Handle Optional[Enum] and Enum | None -> extract Enum from Union
        # In Python 3.10+, X | Y creates a types.UnionType instance
        origin = get_origin(annotation)
        if origin is Union or isinstance(annotation, types.UnionType):
            args = get_args(annotation)
            for arg in args:
                if arg is type(None):  # Skip None type
                    continue
                if isinstance(arg, type) and issubclass(arg, Enum):
                    validators[field_name] = arg
                    break
        # Handle direct Enum
        elif isinstance(annotation, type) and issubclass(annotation, Enum):
            validators[field_name] = annotation

    return validators


# Transform-specific field requirements
# These specify which CoreSurveyResponse fields are needed by each pipeline transform
# All fields referenced here MUST exist in CoreSurveyResponse schema
TRANSFORM_REQUIREMENTS = {
    "auto_sufficiency": {
        "required_together": [["vehicles", "workers"]],  # Both or neither
    },
    "tour_purpose": {
        "required_together": [["orig_purp", "dest_purp"]],  # Both or neither
    },
    "demographics": {
        "required_one_of": [
            # Must have race_dmy columns OR race_cat
            ["race_dmy_ind", "race_dmy_asn", "race_dmy_blk", "race_dmy_hwi", "race_dmy_wht"],
            ["race_cat"],
        ],
    },
    "geocoding": {
        "required_together": [
            ["survey_board_lat", "survey_board_lon"],
            ["survey_alight_lat", "survey_alight_lon"],
        ],
    },
}


def validate_core_required(df: pl.DataFrame) -> list[str]:
    """Validate required core identifier fields exist and are not all null.

    Only the essential identifiers (response_id, survey_id, original_id) are
    truly required for all surveys. Other fields are validated by transform
    requirements when those transforms are enabled.

    Args:
        df: Input DataFrame to validate

    Returns:
        List of error messages (empty if valid)
    """
    errors = []
    # Only the essential identifiers are unconditionally required
    required_identifiers = ["response_id", "survey_id", "original_id"]

    for field in required_identifiers:
        if field not in df.columns:
            errors.append(f"CORE REQUIRED: Missing column '{field}'")
        elif df[field].is_null().all():
            errors.append(f"CORE REQUIRED: Column '{field}' is all null")

    return errors


def validate_pipeline_input_fields(df: pl.DataFrame) -> list[str]:
    """Validate that commonly-used pipeline input fields exist with correct names.

    Warns about suspicious field naming patterns that might indicate mistakes,
    like having survey_time_estimate but not survey_time.

    Args:
        df: Input DataFrame to validate

    Returns:
        List of warning messages (empty if no suspicious patterns detected)
    """
    warnings = []
    present_columns = set(df.columns)

    # Check for common field naming issues (wrong name variants)
    field_variants = {
        "survey_time": ["survey_time_estimate", "survey_time_hour"],
        "orig_purp": ["origin_purpose", "orig_purpose"],
        "dest_purp": ["destination_purpose", "dest_purpose"],
        "access_mode": ["first_access", "access"],
        "egress_mode": ["final_egress", "egress"],
    }

    for expected_field, variants in field_variants.items():
        # If expected field missing but a variant exists, warn
        if expected_field not in present_columns:
            found_variants = [v for v in variants if v in present_columns]
            if found_variants:
                warnings.append(
                    f"PIPELINE INPUTS: Field '{expected_field}' "
                    f"missing but found variant(s): {found_variants}. "
                    f"Pipeline expects '{expected_field}'."
                )

    return warnings


def validate_transform_requirements(  # noqa: C901
    df: pl.DataFrame,
    skip_transforms: list[str] | None = None,
) -> list[str]:
    """Validate transform-specific field requirements.

    Args:
        df: Input DataFrame to validate
        skip_transforms: List of transform names to skip validation for

    Returns:
        List of error messages (empty if valid)
    """
    errors = []
    skip_transforms = skip_transforms or []

    for transform_name, requirements in TRANSFORM_REQUIREMENTS.items():
        if transform_name in skip_transforms:
            continue

        # Check required fields
        if "required" in requirements:
            errors.extend(
                f"TRANSFORM ({transform_name}): Missing required column '{field}'"
                for field in requirements["required"]
                if field not in df.columns
            )

        # Check required_together (all or none)
        if "required_together" in requirements:
            for field_group in requirements["required_together"]:
                present = [f for f in field_group if f in df.columns]
                if present and len(present) != len(field_group):
                    missing = [f for f in field_group if f not in df.columns]
                    errors.append(
                        f"TRANSFORM ({transform_name}): Partial field group present. "
                        f"Have {present}, missing {missing}. "
                        f"Must have all or none of: {field_group}"
                    )

        # Check required_one_of (at least one group must be complete)
        if "required_one_of" in requirements:
            groups = requirements["required_one_of"]
            satisfied = False
            for field_group in groups:
                if all(f in df.columns for f in field_group):
                    satisfied = True
                    break

            if not satisfied:
                errors.append(
                    f"TRANSFORM ({transform_name}): Must have at least one complete field group. "
                    f"Options: {groups}"
                )

    return errors


def validate_enum_values(df: pl.DataFrame) -> list[str]:
    """Validate categorical fields contain only valid enum values.

    Args:
        df: Input DataFrame to validate

    Returns:
        List of error messages (empty if valid)
    """
    errors = []
    enum_validators = extract_enum_validators()

    for field, enum_class in enum_validators.items():
        if field not in df.columns:
            continue

        # Get valid enum values (strings)
        valid_values = {e.value for e in enum_class}

        # Get actual values in data (excluding nulls)
        actual_values = set(df[field].drop_nulls().unique().to_list())

        # Find invalid values
        invalid_values = actual_values - valid_values

        if invalid_values:
            # Show sample of invalid values (max 5)
            sample = list(invalid_values)[:5]
            errors.append(
                f"ENUM VALIDATION: Column '{field}' has invalid values: {sample}. "
                f"Valid values include: {sorted(valid_values)[:10]}..."
            )

    return errors


def validate_preprocessed_input(
    df: pl.DataFrame,
    skip_transforms: list[str] | None = None,
    strict: bool = True,
) -> pl.DataFrame:
    """Validate preprocessed survey data meets pipeline requirements.

    Enforces field contract:
    - Core required: Always required fields (response_id, survey_id, etc.)
    - Transform-specific: Fields required by specific transforms (business logic)
    - Optional: All other CoreSurveyResponse fields

    Args:
        df: Input DataFrame from preprocessing
        skip_transforms: List of transform names to skip (e.g., ["geocoding"])
        strict: If True, raise ValueError on any errors

    Returns:
        Input DataFrame (unmodified)

    Raises:
        ValueError: If validation errors found and strict=True
    """
    all_errors = []

    # Core required: identifiers
    core_errors = validate_core_required(df)
    all_errors.extend(core_errors)

    # Pipeline input fields: check for expected field names
    pipeline_input_warnings = validate_pipeline_input_fields(df)
    all_errors.extend(pipeline_input_warnings)

    # Transform-specific requirements
    transform_errors = validate_transform_requirements(df, skip_transforms=skip_transforms)
    all_errors.extend(transform_errors)

    # Enum validation (categorical fields from model)
    enum_errors = validate_enum_values(df)
    all_errors.extend(enum_errors)

    # Report errors
    if all_errors:
        error_msg = (
            f"\n{'=' * 80}\n"
            f"VALIDATION FAILED: Preprocessed data does not meet pipeline requirements\n"
            f"{'=' * 80}\n"
        )
        for i, error in enumerate(all_errors, 1):
            error_msg += f"\n{i}. {error}"

        error_msg += (
            f"\n\n{'=' * 80}\n"
            f"FIX: Update preprocessing script to provide all required fields\n"
            f"{'=' * 80}\n"
        )

        if strict:
            raise ValueError(error_msg)
        # Log warning in non-strict mode
        logger.warning(error_msg)

    return df


def get_preprocessing_requirements() -> dict[
    str, list[str] | dict[str, dict[str, list[list[str]]]]
]:
    """Get comprehensive list of preprocessing requirements.

    Useful for documentation and preprocessing script development.

    Returns:
        Dictionary with requirement information
    """
    return {
        "core_required": ["response_id", "survey_id", "original_id"],
        "core_fields": extract_core_fields(),
        "transform_requirements": dict(TRANSFORM_REQUIREMENTS.items()),
        "enum_fields": list(extract_enum_validators().keys()),
    }
