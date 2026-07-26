"""
Purpose:
--------
This script is intended for quality control (QC) of consultant survey deliverables.
It verifies the contract criteria by keeping each requirement linked to the
variable or variable group used to measure it.

Usage:
    python check_contract_compliance.py

    Configure file_path and sheet_name at the top of the script before running.

Contract criteria:
- A minimum of 90 percent of all surveys shall identify all specific trip segment operators and routes;
- A minimum of 90 percent of all surveys shall identify each geocoded location including origin,
  boarding, alighting, destination as well as home if home is not the trip origin or destination,
  and work or school if the respondent works or is a student and work or school are not the trip
  origin or destination;
- A minimum of 80 percent of all surveys shall include a response for income;
- A minimum of 85 percent of all surveys shall include a response for race/ethnicity;
- A minimum of 85 percent of all surveys shall include a response for language other than English
  spoken at home;
- A minimum of 85 percent of all surveys shall include a response for number of workers in the
  household;
- A minimum of 85 percent of all surveys shall include a response for number of drivable vehicles
  in the household;
- A minimum of 85 percent of all surveys shall include a response for age; and
- Each survey shall include responses for a minimum of 85 percent of all survey questions asked.

Linked variables:
- route
- orig_lat, orig_lon, first_board_lat, first_board_lon, last_alight_lat, last_alight_lon,
  dest_lat, dest_lon, home_lat, home_lon
- household_income
- race_dmy_ind, race_dmy_hwi, race_dmy_blk, race_dmy_wht, race_dmy_asn, race_dmy_hisp,
  race_other (7 numeric indicator columns; a row is valid if their sum >= 1)
  race_6_other (override: a non-blank value satisfies the criterion regardless of the indicator sum)
- language_at_home_binary
- workers
- vehicles
- year_born_four_digit
- survey completeness across all question columns except excluded administrative fields

Outputs:
- Console summary table of linked criteria, variables, thresholds, and PASS/FAIL.
- Excel workbook written to the same folder as the input file, containing:
    - "Observed Variable Categories": unique non-decimal categories observed for each linked variable.
    - "Contract Summary": one row per variable with threshold, observed rate, and refusal token counts.
    - "Failed Survey Cases": individual survey rows that failed the 85% completeness criterion.

The contract summary also reports counts for each refusal / blank token listed in
REFUSED_TEXT_VALUES and REFUSED_NUMERIC_VALUES.
"""

from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

file_path = r"E:\Box\Modeling and Surveys\Surveys\Transit Passenger Surveys\Ongoing TPS\Individual Operator Efforts\UCT WestCAT 2025 and Marin Transit 2026\MTC_CCG_shared\Task 8 - Survey Dataset\Deliverable 2_8_MTC Marin Transit 2026 Final Data-MTC.xlsx"

sheet_name = "Standardized Data"

# Columns that are not part of the overall survey-completeness calculation.
EXCLUDED_QUESTION_COLUMNS = {"ID", "SURVEY"}

# Questions that are never required for the survey-completeness check.
ALWAYS_OPTIONAL_QUESTION_COLUMNS = {
    "School_Name",
    "Bike_Before",
    "Bike_Before_Type",
    "bike_after",
    "bike_after_type",
    "race_6_other",
    "clipper_detail",
    "number_transfers_orig_board",
    "first_system_before_survey_board",
    "first_route_before_survey_board",
    "1_before_lat_start",
    "1_before_long_start",
    "1_before_lat_end",
    "1_before_long_end",
    "second_system_before_survey_board",
    "second_route_before_survey_board",
    "2_before_lat_start",
    "2_before_long_start",
    "2_before_lat_end",
    "2_before_long_end",
    "third_system_before_survey_board",
    "third_route_before_survey_board",
    "3_before_lat_start",
    "3_before_long_start",
    "3_before_lat_end",
    "3_before_long_end",
    "number_transfers_alight_dest",
    "first_system_after_survey_alight",
    "first_route_after_survey_alight",
    "1_after_lat_start",
    "1_after_long_start",
    "1_after_lat_end",
    "1_after_long_end",
    "second_system_after_survey_alight",
    "second_route_after_survey_alight",
    "2_after_lat_start",
    "2_after_long_start",
    "2_after_lat_end",
    "2_after_long_end",
    "third_system_after_survey_alight",
    "third_route_after_survey_alight",
    "3_after_lat_start",
    "3_after_long_start",
    "3_after_lat_end",
    "3_after_long_end",
}

# Each entry: question_column -> (gating_column, operator, gating_value)
# operator "==": required when gating_column equals gating_value
# operator "!=": required when gating_column does NOT equal gating_value
CONDITIONAL_REQUIRED_COLUMNS = {
    "workplace_lat":            ("work_status",            "==", "yes"),
    "workplace_lon":            ("work_status",            "==", "yes"),
    "at_work_prior_to_orig_purp": ("work_status",          "==", "yes"),
    "at_work_after_dest_purp":  ("work_status",            "==", "yes"),
    "school_lat":               ("student_status",         "!=", "no"),
    "school_lon":               ("student_status",         "!=", "no"),
    "at_school_prior_to_orig_purp": ("student_status",     "!=", "no"),
    "at_school_after_dest_purp": ("student_status",        "!=", "no"),
    "eng_proficient":           ("language_at_home_binary", "!=", "ENGLISH ONLY"),
}

# Values treated as non-responses.
REFUSED_TEXT_VALUES = {"", "<blank>", "refused", "don't know", "dk", "rf"}
REFUSED_NUMERIC_VALUES = {-9, -8, -1, 999, 9999}

REFUSAL_COUNT_COLUMNS = [
    "refused_blank_count",
    "refused_<blank>_count",
    "refused_refused_count",
    "refused_don't_know_count",
    "refused_dk_count",
    "refused_rf_count",
    "refused_-9_count",
    "refused_-8_count",
    "refused_-1_count",
    "refused_999_count",
    "refused_9999_count",
]

# Each criterion keeps the contract text linked to the relevant variable.
CRITERIA = [
    {
        "Criterion": "A minimum of 90 percent of all surveys shall identify all specific trip segment operators and routes.",
        "Linked Variables": ["route"],
        "Threshold": 0.90,
        "Type": "column",
        "Mode": "all",
        "Short Name": "route",
    },
    {
        "Criterion": (
            "A minimum of 90 percent of all surveys shall identify each geocoded location "
            "including origin, boarding, alighting, destination, home, work, or school as applicable."
        ),
        "Linked Variables": [
            "orig_lat",
            "orig_lon",
            "first_board_lat",
            "first_board_lon",
            "last_alight_lat",
            "last_alight_lon",
            "dest_lat",
            "dest_lon",
            "home_lat",
            "home_lon",
        ],
        "Threshold": 0.90,
        "Type": "column",
        "Mode": "all",
        "Short Name": "geocoded_locations",
    },
    {
        "Criterion": "A minimum of 80 percent of all surveys shall include a response for income.",
        "Linked Variables": ["household_income"],
        "Threshold": 0.80,
        "Type": "column",
        "Mode": "all",
        "Short Name": "household_income",
    },
    {
        "Criterion": "A minimum of 85 percent of all surveys shall include a response for race/ethnicity.",
        "Linked Variables": [
            "race_dmy_ind",
            "race_dmy_hwi",
            "race_dmy_blk",
            "race_dmy_wht",
            "race_dmy_asn",
            "race_dmy_hisp",
            "race_other",
            "race_6_other",
        ],
        "Threshold": 0.85,
        "Type": "column",
        "Mode": "race_any_indicator_or_other",
        "Short Name": "race_ethnicity",
    },
    {
        "Criterion": "A minimum of 85 percent of all surveys shall include a response for language other than English spoken at home.",
        "Linked Variables": ["language_at_home_binary"],
        "Threshold": 0.85,
        "Type": "column",
        "Mode": "all",
        "Short Name": "language_at_home_binary",
    },
    {
        "Criterion": "A minimum of 85 percent of all surveys shall include a response for number of workers in the household.",
        "Linked Variables": ["workers"],
        "Threshold": 0.85,
        "Type": "column",
        "Mode": "all",
        "Short Name": "workers",
    },
    {
        "Criterion": "A minimum of 85 percent of all surveys shall include a response for number of drivable vehicles in the household.",
        "Linked Variables": ["vehicles"],
        "Threshold": 0.85,
        "Type": "column",
        "Mode": "all",
        "Short Name": "vehicles",
    },
    {
        "Criterion": "A minimum of 85 percent of all surveys shall include a response for age.",
        "Linked Variables": ["year_born_four_digit"],
        "Threshold": 0.85,
        "Type": "column",
        "Mode": "all",
        "Short Name": "year_born_four_digit",
    },
    {
        "Criterion": "Each survey shall include responses for a minimum of 85 percent of all survey questions asked.",
        "Linked Variables": ["All question columns except excluded administrative fields"],
        "Threshold": 0.85,
        "Type": "survey",
        "Mode": "survey",
        "Short Name": "survey_completeness",
    },
]

def is_missing_or_refused(value: object) -> bool:
    if pd.isna(value):
        return True

    text = str(value).strip().lower()
    if text in REFUSED_TEXT_VALUES:
        return True

    try:
        numeric_value = float(text)
    except ValueError:
        return False

    return numeric_value.is_integer() and int(numeric_value) in REFUSED_NUMERIC_VALUES


def valid_response_mask(series: pd.Series) -> pd.Series:
    return ~series.map(is_missing_or_refused)


def is_non_decimal_category(value: object) -> bool:
    if pd.isna(value):
        return True

    if isinstance(value, (int, float)):
        try:
            return float(value).is_integer()
        except Exception:
            return True

    text = str(value).strip()
    if not text:
        return True

    try:
        numeric_value = float(text)
    except ValueError:
        return True

    return numeric_value.is_integer()


def count_refusal_tokens(series: pd.Series) -> dict[str, int]:
    text_values = series.fillna("").astype(str).str.strip().str.lower()
    return {
        "refused_blank_count": int((text_values == "").sum()),
        "refused_<blank>_count": int((text_values == "<blank>").sum()),
        "refused_refused_count": int((text_values == "refused").sum()),
        "refused_don't_know_count": int((text_values == "don't know").sum()),
        "refused_dk_count": int((text_values == "dk").sum()),
        "refused_rf_count": int((text_values == "rf").sum()),
        "refused_-9_count": int((text_values == "-9").sum()),
        "refused_-8_count": int((text_values == "-8").sum()),
        "refused_-1_count": int((text_values == "-1").sum()),
        "refused_999_count": int((text_values == "999").sum()),
        "refused_9999_count": int((text_values == "9999").sum()),
    }


def normalize_text_value(value: object) -> str:
    return "" if pd.isna(value) else str(value).strip().lower()


def normalize_text_series(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).str.strip().str.lower()


def build_required_question_mask(df: pd.DataFrame, question_cols: list[str]) -> pd.DataFrame:
    required_mask = pd.DataFrame(True, index=df.index, columns=question_cols)

    for column_name in ALWAYS_OPTIONAL_QUESTION_COLUMNS:
        if column_name in required_mask.columns:
            required_mask[column_name] = False

    for question_column, (gating_column, operator, gating_value) in CONDITIONAL_REQUIRED_COLUMNS.items():
        if question_column not in required_mask.columns:
            continue
        if gating_column not in df.columns:
            required_mask[question_column] = True
            continue

        normalized = normalize_text_series(df[gating_column])
        if operator == "==":
            gating_mask = normalized.eq(gating_value.lower())
        else:  # "!="
            gating_mask = normalized.ne(gating_value.lower())

        required_mask[question_column] = gating_mask

    return required_mask


def build_category_inventory(df: pd.DataFrame, criteria: list[dict]) -> pd.DataFrame:
    records = []

    for criterion in criteria:
        for variable in criterion["Linked Variables"]:
            if variable not in df.columns:
                records.append(
                    {
                        "Criterion": criterion["Criterion"],
                        "Linked Variable": variable,
                        "Category": "COLUMN MISSING",
                        "Count": 0,
                        "Percent of Data": "N/A",
                    }
                )
                continue

            value_counts = df[variable].value_counts(dropna=False)
            total_rows = len(df)
            for category, count in value_counts.items():
                if not is_non_decimal_category(category):
                    continue

                records.append(
                    {
                        "Criterion": criterion["Criterion"],
                        "Linked Variable": variable,
                        "Category": "<BLANK>" if pd.isna(category) else category,
                        "Count": int(count),
                        "Percent of Data": f"{(count / total_rows) if total_rows else 0:.2%}",
                    }
                )

    inventory = pd.DataFrame.from_records(records)
    if not inventory.empty:
        inventory = inventory.sort_values(
            by=["Linked Variable", "Count", "Category"], ascending=[True, False, True]
        ).reset_index(drop=True)
    return inventory


def evaluate_group_criterion(
    df: pd.DataFrame, linked_variables: list[str], threshold: float, mode: str
) -> tuple[dict, pd.DataFrame]:
    missing_cols = [col for col in linked_variables if col not in df.columns]
    if missing_cols:
        result = {
            "Criterion": None,
            "Linked Variable(s)": ", ".join(linked_variables),
            "Threshold": f"{threshold:.0%}",
            "Observed Rate": "N/A",
            "Numerator": "N/A",
            "Denominator": len(df),
            "Status": f"COLUMN MISSING ({', '.join(missing_cols)})",
        }
        return result, df.iloc[0:0].copy()

    valid_matrix = df[linked_variables].apply(valid_response_mask)

    if mode == "all":
        valid_mask = valid_matrix.all(axis=1)
    elif mode == "any":
        valid_mask = valid_matrix.any(axis=1)
    else:
        raise ValueError(f"Unsupported criterion mode: {mode}")

    numerator = int(valid_mask.sum())
    denominator = len(df)
    observed_rate = numerator / denominator if denominator > 0 else 0

    result = {
        "Criterion": None,
        "Linked Variable(s)": ", ".join(linked_variables),
        "Threshold": f"{threshold:.0%}",
        "Observed Rate": f"{observed_rate:.2%}",
        "Numerator": numerator,
        "Denominator": denominator,
        "Status": "PASS" if observed_rate >= threshold else "FAIL",
    }

    failed_rows = df.loc[~valid_mask].copy()
    failed_rows["Missing Linked Variable(s)"] = df.loc[~valid_mask, linked_variables].apply(
        lambda row: ", ".join(col for col in linked_variables if is_missing_or_refused(row[col])),
        axis=1,
    )
    return result, failed_rows


def evaluate_race_ethnicity_criterion(
    df: pd.DataFrame, linked_variables: list[str], threshold: float
) -> tuple[dict, pd.DataFrame]:
    missing_cols = [col for col in linked_variables if col not in df.columns]
    if missing_cols:
        result = {
            "Criterion": None,
            "Linked Variable(s)": ", ".join(linked_variables),
            "Threshold": f"{threshold:.0%}",
            "Observed Rate": "N/A",
            "Numerator": "N/A",
            "Denominator": len(df),
            "Status": f"COLUMN MISSING ({', '.join(missing_cols)})",
        }
        return result, df.iloc[0:0].copy()

    indicator_variables = [col for col in linked_variables if col != "race_6_other"]
    indicator_numeric = df[indicator_variables].apply(pd.to_numeric, errors="coerce").fillna(0)
    indicator_sum = indicator_numeric.sum(axis=1)
    race_6_other_nonblank = valid_response_mask(df["race_6_other"])
    valid_mask = (indicator_sum >= 1) | race_6_other_nonblank

    race_6_other_nonblank_count = int(race_6_other_nonblank.sum())
    refusal_mask = (indicator_sum == 0) & ~race_6_other_nonblank

    numerator = int(valid_mask.sum())
    denominator = len(df)
    observed_rate = numerator / denominator if denominator > 0 else 0

    result = {
        "Criterion": None,
        "Linked Variable(s)": ", ".join(linked_variables),
        "Threshold": f"{threshold:.0%}",
        "Observed Rate": f"{observed_rate:.2%}",
        "Numerator": numerator,
        "Denominator": denominator,
        "Status": "PASS" if observed_rate >= threshold else "FAIL",
        "refused_race_var_sum_to_0_count": int(refusal_mask.sum()),
    }

    failed_rows = df.loc[~valid_mask].copy()
    failed_rows["Missing Linked Variable(s)"] = df.loc[~valid_mask, linked_variables].apply(
        lambda row: ", ".join(
            col
            for col in linked_variables
            if is_missing_or_refused(row[col])
        ),
        axis=1,
    )
    return result, failed_rows


def evaluate_single_variable(
    df: pd.DataFrame, criterion_text: str, variable: str, threshold: float
) -> dict:
    if variable not in df.columns:
        result = {
            "Criterion": criterion_text,
            "Linked Variable(s)": variable,
            "Threshold": f"{threshold:.0%}",
            "Observed Rate": "N/A",
            "Numerator": "N/A",
            "Denominator": len(df),
            "Status": "COLUMN MISSING",
        }
        for column_name in REFUSAL_COUNT_COLUMNS:
            result[column_name] = "N/A"
        return result

    valid_mask = valid_response_mask(df[variable])
    numerator = int(valid_mask.sum())
    denominator = len(df)
    observed_rate = numerator / denominator if denominator > 0 else 0

    result = {
        "Criterion": criterion_text,
        "Linked Variable(s)": variable,
        "Threshold": f"{threshold:.0%}",
        "Observed Rate": f"{observed_rate:.2%}",
        "Numerator": numerator,
        "Denominator": denominator,
        "Status": "PASS" if observed_rate >= threshold else "FAIL",
    }

    result.update(count_refusal_tokens(df[variable]))
    return result


def evaluate_survey_completeness(
    df: pd.DataFrame, threshold: float
) -> tuple[dict, pd.DataFrame]:
    question_cols = [col for col in df.columns if col not in EXCLUDED_QUESTION_COLUMNS]

    if not question_cols:
        result = {
            "Criterion": None,
            "Linked Variable(s)": "All question columns except excluded administrative fields",
            "Threshold": f"{threshold:.0%}",
            "Observed Rate": "N/A",
            "Numerator": "N/A",
            "Denominator": len(df),
            "Status": "NO QUESTION COLUMNS",
        }
        for column_name in REFUSAL_COUNT_COLUMNS:
            result[column_name] = "N/A"
        return result, df.iloc[0:0].copy()

    required_mask = build_required_question_mask(df, question_cols)
    valid_matrix = df[question_cols].apply(valid_response_mask)
    applicable_matrix = required_mask
    answered_counts = (valid_matrix & applicable_matrix).sum(axis=1)
    required_counts = applicable_matrix.sum(axis=1)
    completeness_rate = answered_counts / required_counts.replace(0, pd.NA)
    completeness_rate = completeness_rate.fillna(1)
    passed_mask = completeness_rate >= threshold

    numerator = int(passed_mask.sum())
    denominator = len(df)
    observed_rate = numerator / denominator if denominator > 0 else 0

    result = {
        "Criterion": None,
        "Linked Variable(s)": "All question columns except excluded administrative fields",
        "Threshold": f"{threshold:.0%}",
        "Observed Rate": f"{observed_rate:.2%}",
        "Numerator": numerator,
        "Denominator": denominator,
        "Status": "PASS" if observed_rate >= threshold else "FAIL",
    }

    for column_name in REFUSAL_COUNT_COLUMNS:
        result[column_name] = "N/A"

    failed_rows = df.loc[~passed_mask].copy()
    failed_rows["Answered Questions"] = answered_counts.loc[~passed_mask].values
    failed_rows["Total Questions"] = required_counts.loc[~passed_mask].values
    failed_rows["Completeness Rate"] = completeness_rate.loc[~passed_mask].map(lambda value: f"{value:.2%}")
    return result, failed_rows


# ---------------------------------------------------------------------------
# Read data
# ---------------------------------------------------------------------------

df = pd.read_excel(file_path, sheet_name=sheet_name)

num_rows = len(df)
print(f"Rows read from '{sheet_name}': {num_rows:,}\n")

# ---------------------------------------------------------------------------
# Check that all expected columns exist
# ---------------------------------------------------------------------------

missing_cols = [
    linked_variable
    for criterion in CRITERIA
    for linked_variable in criterion["Linked Variables"]
    if criterion["Type"] == "column" and linked_variable not in df.columns
]
if missing_cols:
    print("WARNING: The following expected columns were NOT found in the data:")
    for col in missing_cols:
        print(f"  - {col}")
    print()

# ---------------------------------------------------------------------------
# Evaluate contract criteria
# ---------------------------------------------------------------------------

results = []
survey_completeness_failed_rows = None

for criterion in CRITERIA:
    criterion_text = criterion["Criterion"]
    linked_variables = criterion["Linked Variables"]
    threshold = criterion["Threshold"]

    if criterion["Type"] == "column":
        if criterion["Short Name"] == "race_ethnicity":
            summary_row, _ = evaluate_race_ethnicity_criterion(df, linked_variables, threshold)
            summary_row["Criterion"] = criterion_text
            results.append(summary_row)
        else:
            for variable in linked_variables:
                summary_row = evaluate_single_variable(df, criterion_text, variable, threshold)
                results.append(summary_row)
    else:
        eval_result, failed_rows = evaluate_survey_completeness(df, threshold)
        eval_result["Criterion"] = criterion_text
        eval_result["Linked Variable(s)"] = linked_variables[0]
        eval_result["Threshold"] = f"{threshold:.0%}"
        results.append(eval_result)
        survey_completeness_failed_rows = failed_rows

summary_df = pd.DataFrame(results)

summary_df = summary_df[
    [
        "Criterion",
        "Linked Variable(s)",
        "Threshold",
        "Observed Rate",
        "Numerator",
        "Denominator",
        "Status",
        "refused_race_var_sum_to_0_count",
        *REFUSAL_COUNT_COLUMNS,
    ]
]

# ---------------------------------------------------------------------------
# Print summary
# ---------------------------------------------------------------------------

col_widths = {
    "Criterion": 95,
    "Linked Variable(s)": 30,
    "Threshold": 10,
    "Observed Rate": 14,
    "Numerator": 12,
    "Denominator": 12,
    "Status": 16,
}

header = "  ".join(k.ljust(v) for k, v in col_widths.items())
divider = "-" * len(header)
print(header)
print(divider)

for _, row in summary_df.iterrows():
    line = "  ".join(str(row[k]).ljust(v) for k, v in col_widths.items())
    print(line)

if survey_completeness_failed_rows is not None and not survey_completeness_failed_rows.empty:
    print("\nFailed survey-completeness cases:")
    failure_columns = [
        col
        for col in ["ID", "Answered Questions", "Total Questions", "Completeness Rate"]
        if col in survey_completeness_failed_rows.columns
    ]
    if failure_columns:
        failed_preview = survey_completeness_failed_rows[failure_columns].copy()
        if len(failed_preview) > 25:
            print(failed_preview.head(25).to_string(index=True))
            print(f"... showing 25 of {len(failed_preview)} failed cases")
        else:
            print(failed_preview.to_string(index=True))
    else:
        print(survey_completeness_failed_rows.to_string(index=True))

# ---------------------------------------------------------------------------
# Export summary to Excel
# ---------------------------------------------------------------------------

output_summary = Path(file_path).parent / f"{sheet_name.replace(' ', '_')}_Check_contract_requirements.xlsx"
category_inventory_df = build_category_inventory(df, CRITERIA)

with pd.ExcelWriter(output_summary, engine="openpyxl") as writer:
    category_inventory_df.to_excel(writer, sheet_name="Observed Variable Categories", index=False)
    summary_df.to_excel(writer, sheet_name="Contract Summary", index=False)
    if survey_completeness_failed_rows is not None:
        survey_completeness_failed_rows.to_excel(
            writer, sheet_name="Failed Survey Cases", index=False
        )

print(f"\nSummary exported to:\n{output_summary}")
