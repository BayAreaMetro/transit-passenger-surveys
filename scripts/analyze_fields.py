"""Analyze field usage and generate statistics for survey data."""

import logging
import numbers
from pathlib import Path

import polars as pl

from transit_passenger_tools.database import connect

LOGGER = logging.getLogger(__name__)

# Data sources
LEGACY_CSV = Path(
    r"\\\\models.ad.mtc.ca.gov\data\models\Data\OnBoard\Data and Reports"
    r"\_data_Standardized\standardized_2025-10-13\survey_combined.csv"
)
OUTPUT_DIR = Path("scripts/field_analysis")


def load_data(source: str | Path) -> pl.DataFrame:
    """Load data from CSV file or DuckDB database."""
    if str(source).endswith(".csv"):
        return pl.read_csv(source, null_values=["NA", "N/A", "", "Inf"], infer_schema_length=10000)
    conn = connect(read_only=True)
    df = conn.execute("SELECT * FROM survey_responses").pl()
    conn.close()
    return df


def analyze_fields(df: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Generate field usage statistics and codebook."""
    total_rows = len(df)
    field_stats = []
    codebook_entries = []

    for col in df.columns:
        null_count = df[col].null_count()
        non_null_pct = (total_rows - null_count) / total_rows * 100
        dtype = str(df[col].dtype)

        stats = {
            "field": col,
            "non_null_percent": round(non_null_pct, 2),
            "data_type": dtype,
            "min": None,
            "max": None,
            "mean": None,
            "stddev": None,
            "sum": None,
        }

        # Calculate numeric statistics
        if df[col].dtype in (pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.Float32, pl.Float64):
            try:
                stats["min"] = df[col].min()
                stats["max"] = df[col].max()
                mean_value = df[col].mean()
                std_value = df[col].std()
                stats["mean"] = (
                    round(float(mean_value), 4) if isinstance(mean_value, numbers.Real) else None
                )
                stats["stddev"] = (
                    round(float(std_value), 4) if isinstance(std_value, numbers.Real) else None
                )
                stats["sum"] = df[col].sum()
            except Exception:
                LOGGER.exception("Failed to compute numeric stats for column %s", col)

        field_stats.append(stats)

        # Generate codebook for categorical fields
        n_unique = df[col].n_unique()
        if n_unique <= 25:  # noqa: PLR2004
            value_counts = df[col].value_counts(sort=True)
            codebook_entries.extend(
                {
                    "field": col,
                    "value": row[col],
                    "count": row["count"],
                    "percent": round(row["count"] / total_rows * 100, 2),
                }
                for row in value_counts.iter_rows(named=True)
            )

    field_stats_df = pl.DataFrame(field_stats)
    codebook_df = pl.DataFrame(codebook_entries) if codebook_entries else pl.DataFrame()

    return field_stats_df, codebook_df


if __name__ == "__main__":
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    sources = [("database", "database"), ("legacy", LEGACY_CSV)]

    for source_name, source_path in sources:
        LOGGER.info("=" * 60)
        LOGGER.info("Analyzing %s source...", source_name)
        LOGGER.info("=" * 60)

        df = load_data(source_path)
        LOGGER.info("Loaded %s rows with %s columns", len(df), len(df.columns))

        LOGGER.info("Analyzing fields...")
        field_stats_df, codebook_df = analyze_fields(df)

        field_usage_path = OUTPUT_DIR / f"field_usage_{source_name}.csv"
        field_stats_df.write_csv(field_usage_path)
        LOGGER.info("Written: %s", field_usage_path)

        if len(codebook_df) > 0:
            codebook_path = OUTPUT_DIR / f"field_codebook_{source_name}.csv"
            codebook_df.write_csv(codebook_path)
            LOGGER.info("Written: %s", codebook_path)

        LOGGER.info("=" * 60)
        LOGGER.info("Analysis complete!")
        LOGGER.info("=" * 60)
