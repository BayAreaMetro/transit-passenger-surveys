"""Append routed access/egress distances to the BART 2024 final deliverable file.

Writes a new copy of the final MTC deliverable workbook (suffix ``_routed``)
with two extra columns joined on from the canonical warehouse's
``survey_responses`` table:

- ``distance_orig_first_board_routed``
- ``distance_last_alight_dest_routed``

The original workbook is never modified. The workbook's ``UNIQUE_IDENTIFIER``
maps to the warehouse ``response_id`` as ``f"BART_2024_{UNIQUE_IDENTIFIER}"``.

A describing row for each new column is appended to the codebook sheet; the
ReadMe sheet is copied through verbatim. Cell styling is not preserved — only
values.

The workbook is built on local disk and moved into place only once complete.
Writing a large file incrementally into a Box-synced folder races the Box
cloud-filter driver, which forks a conflict copy and leaves a 0-byte original.
"""

import logging
import shutil
import tempfile
from pathlib import Path

import fastexcel
import polars as pl
import xlsxwriter

from transit_passenger_tools.database import read_responses

logger = logging.getLogger(__name__)

# Mixed-type columns legitimately fall back to string; the per-column warnings
# are just noise.
logging.getLogger("fastexcel").setLevel(logging.ERROR)

CANONICAL_OPERATOR = "BART"
SURVEY_YEAR = 2024
DATA_SHEET = "StationProfileV1_2_DataRev03182"
CODEBOOK_SHEET = "codebook"
ID_COLUMN = "UNIQUE_IDENTIFIER"

# variable -> codebook description. The codebook sheet is headerless with four
# columns: variable, description, value, value label. The two value columns stay
# blank for these, as they do for every other continuous variable.
ROUTED_COLUMNS = {
    "distance_orig_first_board_routed": (
        "Network (routed) distance in miles from the trip origin to the first boarding "
        "location, computed as shortest path by MTC via OSRM. "
        "The routing profile is mode-aware: it follows the street/path network for "
        "the respondent's reported access mode (drive, walk, or bike). "
        "Blank where the origin or boarding location could not be geocoded or routed."
    ),
    "distance_last_alight_dest_routed": (
        "Network (routed) distance in miles from the last alighting location to the trip "
        "destination, computed as shortest path by MTC via OSRM. "
        "The routing profile is mode-aware: it follows the street/path network for the "
        "respondent's reported egress mode (drive, walk, or bike). "
        "Blank where the alighting location or destination could not be geocoded or "
        "routed."
    ),
}

SURVEY_PATH = Path(
    r"E:\Box\Modeling and Surveys\Surveys\Transit Passenger Surveys"
    r"\Ongoing TPS\Individual Operator Efforts\BART 2024"
    r"\BART MTC ETC RSG Project Folder\Final Data and Report\v1_2 Data File"
    r"\StationProfileV1_2_Rev031826_Numeric_forMTC.xlsx"
)
OUTPUT_PATH = SURVEY_PATH.with_name(SURVEY_PATH.stem + "_routed" + SURVEY_PATH.suffix)


def _append_codebook_rows(codebook: pl.DataFrame) -> pl.DataFrame:
    """Append a describing row for each routed column to the *codebook* sheet."""
    variable_col, description_col = codebook.columns[:2]
    new_rows = pl.DataFrame(
        {
            variable_col: list(ROUTED_COLUMNS.keys()),
            description_col: list(ROUTED_COLUMNS.values()),
        },
    )
    return pl.concat([codebook, new_rows], how="diagonal")


def append_routed_distances(survey_path: Path, output_path: Path) -> None:
    """Copy *survey_path* to *output_path* with the routed distance columns appended."""
    routed = read_responses(operator=CANONICAL_OPERATOR, year=SURVEY_YEAR).select(
        pl.col("response_id")
        .str.strip_prefix(f"{CANONICAL_OPERATOR}_{SURVEY_YEAR}_")
        .alias(ID_COLUMN),
        *ROUTED_COLUMNS,
    )

    reader = fastexcel.read_excel(survey_path)

    with tempfile.TemporaryDirectory() as tmpdir:
        staged = Path(tmpdir) / output_path.name

        # Note: xlsxwriter's constant_memory mode is NOT usable here — Polars
        # emits each sheet via add_table(), which that mode silently discards,
        # producing an empty workbook.
        with xlsxwriter.Workbook(staged) as workbook:
            for name in reader.sheet_names:
                # Headerless read: every sheet round-trips verbatim, row for row.
                sheet = reader.load_sheet_by_name(
                    name, header_row=None if name != DATA_SHEET else 0
                ).to_polars()

                if name == DATA_SHEET:
                    sheet = sheet.join(routed, on=ID_COLUMN, how="left")
                    for col in ROUTED_COLUMNS:
                        logger.info(
                            "%s: %s of %s rows populated",
                            col,
                            f"{sheet[col].is_not_null().sum():,}",
                            f"{len(sheet):,}",
                        )
                elif name == CODEBOOK_SHEET:
                    sheet = _append_codebook_rows(sheet)

                sheet.write_excel(
                    workbook=workbook, worksheet=name, include_header=name == DATA_SHEET
                )

        shutil.move(staged, output_path)

    logger.info("Saved %s", output_path)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    append_routed_distances(SURVEY_PATH, OUTPUT_PATH)
