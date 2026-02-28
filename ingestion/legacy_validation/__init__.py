"""Legacy validation module for comparing preprocessing output against legacy database.

Provides reusable validation logic for any survey to validate new preprocessing
against existing database records created by legacy R-based processing.

Components:
- compare: Main comparison orchestration and data loading
- reports: Field-by-field comparison and summary report generation
- analysis: Derived field root cause analysis

Example usage:
    from legacy_validation import compare
    from preprocessing import preprocess

    # Just process through pipeline
    df = compare.process_pipeline(
        preprocess_func=preprocess,
        operator="ACE",
        year=2023
    )

    # Process and validate against legacy
    compare.compare_outputs(
        operator="ACE",
        year=2023,
        preprocess_func=preprocess,
        output_dir="output"
    )
"""

from .compare import compare_outputs, load_legacy_data, process_pipeline

__all__ = ["compare_outputs", "load_legacy_data", "process_pipeline"]
