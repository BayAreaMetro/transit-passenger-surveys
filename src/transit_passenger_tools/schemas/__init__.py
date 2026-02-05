"""Pydantic data models and enumerated types for transit survey data."""

from dataclasses import dataclass, field


@dataclass
class FieldDependencies:
    """Declares input and output fields for a pipeline module.

    Attributes:
        inputs: Fields the module reads (from Core or other derived fields)
        outputs: Fields the module produces
        optional_inputs: Fields that may be used if present (default: [])
    """

    inputs: list[str]
    outputs: list[str]
    optional_inputs: list[str] = field(default_factory=list)
