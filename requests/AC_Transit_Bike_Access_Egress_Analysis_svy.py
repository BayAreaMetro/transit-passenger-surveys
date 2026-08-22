# ============================================================
# AC Transit – Access/Egress Mode Share Analysis
# svy-library version
#
# Estimation uses the svy package (Taylor linearization via
# svy.Sample + estimation.mean()).  Singleton strata are handled
# with scale(), which drops them from the variance then inflates
# by 1 / (1 - singleton_fraction) to compensate.
#
# NOTE on singleton handling:
#   Both scripts use variance scaling for singleton strata:
#     R:   options(survey.lonely.psu = "average")
#     svy: sample.singleton.scale()
#   Singleton strata are dropped from the variance sum, then the result is
#   scaled up by 1 / (1 - singleton_fraction) to compensate.
# ============================================================

import re
import polars as pl
import pandas as pd
import svy

# ── Configuration ────────────────────────────────────────────
FILE_PATH   = r"E:\Box\Modeling and Surveys\Surveys\Transit Passenger Surveys\Ongoing TPS\Individual Operator Efforts\AC Transit 2025 (OD Survey)\AC_Transit_MTC_ETC_Shared_Folder\Survey Databases\Final\od_20260318_ac-transit_weighted-secondary-weekend 1.xlsx"
OUTPUT_FILE = r"E:\Box\Modeling and Surveys\Surveys\Requests\CART_TDM_AC_Transit_BikeAcessEgress\AC_Transit_BikeAcessEgress_svy.csv"

# Each filter is a label → (column_to_search, pattern_to_match, case_sensitive)
ROUTE_FILTERS = {
    "E/P/V/NX/NX3 routes": ("ROUTE_DIRECTION[Code]", "_E_|_P_|_V_|_NX3_|_NX_", True),
    "All Transbay routes":  ("ROUTE_DIRECTION",       "Transbay",                False),
}

BIKE_SCOOTER_MODES = ["Personal Bike", "Bike Share", "Personal E-scooter", "E-scooter share"]
Z_VALUE    = 1.96   # normal approximation for 95% CI

# Day-type expansion factors for an average-day weight.
# Weekday records are multiplied by 5 and weekend by 2, then divided by 7
# so that the adjusted weight represents one average day of the week.
DAY_WEIGHTS = {"Weekday": 5, "Weekend": 2}

# Variables to analyse: label (used in metric name) → column in the data.
# Add or remove rows here to change what gets summarised.
ANALYSIS_VARS = {
    "Access": "ORIGIN_TRANSPORT",
    "Egress": "DESTIN_TRANSPORT",
}

# ── Helpers ──────────────────────────────────────────────────
def safe_col(prefix, text):
    """Turn a transport mode label into a safe column name."""
    return prefix + re.sub(r"[^a-zA-Z0-9]", "_", text).strip("_")


def build_mode_metrics(df):
    """
    For every variable in ANALYSIS_VARS, create a binary indicator column for
    each unique value and add it to the metrics dict.
    'Bike/Scooter Access or Egress (combined)' is appended at the end.
    """
    metrics = {}

    for label, data_col in ANALYSIS_VARS.items():
        for mode in sorted(df[data_col].dropna().unique()):
            col = safe_col(f"{label.lower()}__", mode)
            df[col] = (df[data_col] == mode).astype(int)
            metrics[f"{label}: {mode}"] = col

    df["bike_any"] = (
        df["ORIGIN_TRANSPORT"].isin(BIKE_SCOOTER_MODES) |
        df["DESTIN_TRANSPORT"].isin(BIKE_SCOOTER_MODES)
    ).astype(int)
    metrics["Bike/Scooter Access or Egress (combined)"] = "bike_any"

    return metrics


def estimate_share(sample, df, col):
    """
    Weighted mean share + Wald CI for a binary 0/1 column via svy.

    Uses estimation.mean() to match R's survey_mean() formula:
    est = sum(w * y) / sum(w), the Horvitz-Thompson estimator.
    SE and CV come from svy's Taylor linearization.
    CI bounds are overridden with the normal approximation (z = 1.96)
    to match R's mutate(ci_lower_95 = weighted_share - 1.96 * se).
    """
    mean_row = sample.estimation.mean(y=col).to_polars().to_dicts()[0]

    est = mean_row["est"]
    se  = mean_row["se"]
    lci = est - Z_VALUE * se
    uci = est + Z_VALUE * se

    return {
        "weighted_share":   est,
        "se":               se,
        "ci_lower_95":      lci,
        "ci_upper_95":      uci,
        "ci_95":            uci - est,
        "coeff_of_var":     mean_row["cv"],
        "weighted_count":   df.loc[df[col] == 1, "ADJ_WGHT_FCTR"].sum(),
        "total_weighted":   df["ADJ_WGHT_FCTR"].sum(),
        "unweighted_count": int(df[col].sum()),
        "total_unweighted": len(df),
    }


def reliability_label(row):
    if row["cv_flag"]:          return "Poor (High CV >30%)"
    if row["sample_size_flag"]: return "Poor (Small sample n<30)"
    if row["ci_width_flag"]:    return "Poor (Wide CI >40pp)"
    return "Acceptable"


def run_analysis(df_full, filter_label, filter_col, filter_pattern, case_sensitive):
    """Run the full analysis for one route filter. Returns a results DataFrame."""
    print(f"\n{'='*60}")
    print(f"Route filter: {filter_label}")

    # ── Filter rows ───────────────────────────────────────────
    df = df_full[
        df_full[filter_col].str.contains(filter_pattern, case=case_sensitive, na=False)
    ].copy()
    print(f"Records after filter: {len(df)}")

    # ── Matched routes summary ────────────────────────────────
    routes_file = OUTPUT_FILE.replace(".csv", f"_matched_routes_{filter_label.replace('/', '-')}.csv")
    matched = (
        df.groupby(["ROUTE_DIRECTION[Code]", "ROUTE_DIRECTION"])
        .size().reset_index(name="n_records").sort_values("ROUTE_DIRECTION")
    )
    matched.to_csv(routes_file, index=False)
    print(f"Matched routes written to {routes_file}")
    print(matched.to_string(index=False))

    # ── Strata & drop incomplete rows ─────────────────────────
    df["stratum"] = (
        df["ROUTE_DIRECTION"].astype(str) + " | " +
        df["TIME_PERIOD"].astype(str)     + " | " +
        df["DATE_TYPE"].astype(str)
    )
    n_before = len(df)
    df = df.dropna(subset=["TIME_PERIOD", "DATE_TYPE", "LINKED_WGHT_FCTR"])
    print(f"Dropped {n_before - len(df)} rows with NA in strata/weight; {len(df)} remaining")

    # ── Average-day weight adjustment ────────────────────────
    # Scale the survey weight so each record represents one average day:
    #   weekday records × 5  (Mon–Fri)
    #   weekend records × 2  (Sat–Sun)
    #   divide by 7          (days in a week)
    df["ADJ_WGHT_FCTR"] = (
        df["LINKED_WGHT_FCTR"] * df["DATE_TYPE"].map(DAY_WEIGHTS) / 7
    )

    # ── Build metrics (adds binary columns to df) ─────────────
    metrics = build_mode_metrics(df)

    # ── Survey design ─────────────────────────────────────────
    # Pass only the columns svy needs to avoid type-conversion issues
    # with mixed-type Excel columns when converting to Polars.
    # No PSU: vehicle ID not available.
    svy_cols = ["stratum", "ADJ_WGHT_FCTR"] + list(metrics.values())
    sample = svy.Sample(
        data   = pl.from_pandas(df[svy_cols]),
        design = svy.Design(stratum="stratum", wgt="ADJ_WGHT_FCTR"),
    )

    # Singleton handling: scale() drops singleton strata from the variance then
    # inflates the result by 1 / (1 − singleton_fraction), matching R's
    # options(survey.lonely.psu = "average").
    sample = sample.singleton.scale()

    # ── Estimate shares ───────────────────────────────────────
    results_rows = []
    for label, col in metrics.items():
        row = estimate_share(sample, df, col)
        row["metric"]       = label
        row["route_filter"] = filter_label
        results_rows.append(row)

    results_df = pd.DataFrame(results_rows)

    # ── Reliability flags ─────────────────────────────────────
    results_df["cv_flag"]          = results_df["coeff_of_var"] > 0.30
    results_df["sample_size_flag"] = results_df["total_unweighted"] < 30
    results_df["ci_width_flag"]    = (results_df["ci_upper_95"] - results_df["ci_lower_95"]) > 0.40
    results_df["suppress"]         = (
        results_df["cv_flag"] | results_df["sample_size_flag"] | results_df["ci_width_flag"]
    )
    results_df["estimate_reliability"] = results_df.apply(reliability_label, axis=1)

    # ── Print summary ─────────────────────────────────────────
    print(f"\n── Results: {filter_label} ──\n")
    for _, row in results_df.iterrows():
        print(
            f"  {row['metric']:<45}  {row['weighted_share']*100:5.1f}%  "
            f"(95% CI: {row['ci_lower_95']*100:5.1f}% \u2013 {row['ci_upper_95']*100:5.1f}%)  "
            f"[n={int(row['unweighted_count'])}]  {row['estimate_reliability']}"
        )

    return results_df


# ── Main ─────────────────────────────────────────────────────
df_full = pd.read_excel(FILE_PATH, sheet_name="OD_RESULTS")
print(f"Loaded {len(df_full)} rows from OD_RESULTS")

all_results = []
for filter_label, (filter_col, filter_pattern, case_sensitive) in ROUTE_FILTERS.items():
    result = run_analysis(df_full, filter_label, filter_col, filter_pattern, case_sensitive)
    all_results.append(result)

combined = pd.concat(all_results, ignore_index=True)

col_order = [
    "route_filter", "metric", "weighted_share", "se", "ci_95",
    "ci_lower_95", "ci_upper_95", "coeff_of_var",
    "weighted_count", "unweighted_count", "total_weighted",
    "total_unweighted", "estimate_reliability",
]
combined = combined[col_order]

combined.to_csv(OUTPUT_FILE, index=False)
print(f"\nWrote {len(combined)} rows to {OUTPUT_FILE}")
