#!/usr/bin/env python
"""Summarize which fields are always blank for rows where SURVEY is "SAS" or "Modelling", to compare the two instruments.

Usage:
    python bart_sas_blank_fields.py "E:\path\to\StationProfileV1_2_Rev031826_Labels_forMTC.xlsx"
    python bart_sas_blank_fields.py "E:\Box\Modeling and Surveys\Surveys\Transit Passenger Surveys\Ongoing TPS\Individual Operator Efforts\BART 2024\BART MTC ETC RSG Project Folder\Final Data and Report\v1_2 Data File\StationProfileV1_2_Rev031826_Labels_forMTC.xlsx"
    python bart_sas_blank_fields.py "...xlsx" --sheet "Sheet1"

The script writes a CSV summary next to the workbook and also prints the
fields that are always blank when SURVEY is "SAS" or "Modelling".

The script treats cells containing only whitespace as blank and also treats
NaN / None as blank.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


BLANK_VALUES = {"", "na", "n/a", "null", "none"}
DEFAULT_SHEET = "StationProfileV1_2_Rev031826"
SURVEY_VALUES = ("SAS", "Modelling")


def is_blank(value: object) -> bool:
    if pd.isna(value):
        return True
    if isinstance(value, str):
        return value.strip().lower() in BLANK_VALUES
    return False


def blank_summary(df: pd.DataFrame, survey_value: str = "SAS") -> pd.DataFrame:
    if "SURVEY" not in df.columns:
        raise KeyError("The workbook does not contain a SURVEY column.")

    survey_mask = df["SURVEY"].astype(str).str.strip().eq(survey_value)
    sas_rows = df.loc[survey_mask].copy()

    if sas_rows.empty:
        return pd.DataFrame(
            columns=[
                "field",
                "row_count_for_this_survey_type",
                "blank_count",
                "nonblank_count",
                "always_blank",
            ]
        )

    records = []
    for column in df.columns:
        if column == "SURVEY":
            continue
        values = sas_rows[column]
        blank_mask = values.map(is_blank)
        blank_count = int(blank_mask.sum())
        nonblank_count = int((~blank_mask).sum())
        records.append(
            {
                "field": column,
                "row_count_for_this_survey_type": int(len(sas_rows)),
                "blank_count": blank_count,
                "nonblank_count": nonblank_count,
                "always_blank": nonblank_count == 0,
            }
        )

    result = pd.DataFrame.from_records(records)
    result = result.sort_values(
        by=["always_blank", "blank_count", "field"], ascending=[False, False, True]
    ).reset_index(drop=True)
    return result


def write_summary_csv(df: pd.DataFrame, output_csv: Path) -> None:
    df.to_csv(output_csv, index=False)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Summarize fields that are always blank when SURVEY == SAS."
    )
    parser.add_argument("workbook", type=Path, help="Path to the .xlsx workbook")
    parser.add_argument(
        "--sheet",
        default=DEFAULT_SHEET,
        help=f'Worksheet to inspect. Default: "{DEFAULT_SHEET}"',
    )
    parser.add_argument(
        "--show-all",
        action="store_true",
        help="Print all fields instead of only always-blank fields.",
    )
    args = parser.parse_args()

    if not args.workbook.exists():
        raise FileNotFoundError(f"Workbook not found: {args.workbook}")

    output_csvs = {
        survey_value: args.workbook.with_name(f"{args.sheet}_{survey_value.lower()}_blank_summary.csv")
        for survey_value in SURVEY_VALUES
    }

    try:
        df = pd.read_excel(args.workbook, sheet_name=args.sheet)
    except ValueError as exc:
        print(f'Could not read sheet "{args.sheet}": {exc}')
        return 1

    summaries = []
    for survey_value in SURVEY_VALUES:
        try:
            summary = blank_summary(df, survey_value=survey_value)
        except KeyError as exc:
            print(f'[{args.sheet}] {exc}')
            return 1

        summary = summary.assign(survey=survey_value)
        summaries.append(summary)

        row_count = int(summary["row_count_for_this_survey_type"].iloc[0]) if not summary.empty else 0
        always_blank = summary[summary["always_blank"]]

        print(f'[{args.sheet}] {survey_value} rows: {row_count}')
        if summary.empty:
            print(f"  No {survey_value} rows found.")
            continue

        report = summary if args.show_all else always_blank

        if report.empty:
            print(f"  No fields are always blank for {survey_value}.")
            continue

        print(f"  Fields always blank for {survey_value}:")
        for _, row in report.iterrows():
            print(f"    - {row['field']}")

        if args.show_all:
            print("  Full summary:")
            for _, row in summary.iterrows():
                status = "always blank" if row["always_blank"] else "not always blank"
                print(
                    f"    - {row['field']}: {row['blank_count']}/{row['row_count_for_this_survey_type']} blank ({status})"
                )

        write_summary_csv(summary, output_csvs[survey_value])
        print(f"  Wrote CSV: {output_csvs[survey_value]}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
