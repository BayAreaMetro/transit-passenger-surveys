# ============================================================
# AC Transit – Access/Egress Mode Share Analysis
# R equivalent of AC_Transit_Bike_Access_Egress_Analysis_svy.py
#
# Analyses all transport modes across multiple route filters.
# Singleton strata handled with survey.lonely.psu = "average"
# (equivalent to svy's scale(): drops singletons from variance,
# then scales up by 1 / (1 - singleton_fraction)).
# ============================================================

library(glue)
library(readxl)
library(dplyr)
library(srvyr)

# Handle lonely PSUs by scaling up the variance from non-singleton strata
# to compensate for the dropped singletons (R's "average" = svy's scale()).
options(survey.lonely.psu = "average")

# ── Configuration ────────────────────────────────────────────
file_path   <- r"(E:\Box\Modeling and Surveys\Surveys\Transit Passenger Surveys\Ongoing TPS\Individual Operator Efforts\AC Transit 2025 (OD Survey)\AC_Transit_MTC_ETC_Shared_Folder\Survey Databases\Final\od_20260318_ac-transit_weighted-secondary-weekend 1.xlsx)"
output_file <- r"(E:\Box\Modeling and Surveys\Surveys\Requests\CART_TDM_AC_Transit_BikeAcessEgress\AC_Transit_BikeAcessEgress_srvyr.csv)"

# Each filter: label, column to search, regex pattern, ignore case flag
route_filters <- list(
  list(label = "E/P/V/NX/NX3 routes", col = "ROUTE_DIRECTION[Code]", pattern = "_E_|_P_|_V_|_NX3_|_NX_", ignore_case = FALSE),
  list(label = "All Transbay routes",  col = "ROUTE_DIRECTION",       pattern = "Transbay",               ignore_case = TRUE)
)

bike_scooter_modes <- c("Personal Bike", "Bike Share", "Personal E-scooter", "E-scooter share")
Z_VALUE <- 1.96   # normal approximation for 95% CI

# Day-type expansion factors for an average-day weight.
# Weekday records are multiplied by 5 and weekend by 2, then divided by 7
# so that the adjusted weight represents one average day of the week.
day_weights <- c("Weekday" = 5, "Weekend" = 2)

# Variables to analyse: label → column in the data.
# Add or remove entries here to change what gets summarised.
analysis_vars <- list(
  "Access" = "ORIGIN_TRANSPORT",
  "Egress" = "DESTIN_TRANSPORT"
)

# ── Helpers ──────────────────────────────────────────────────
safe_col <- function(prefix, text) {
  # Turn a transport mode label into a safe column name.
  col <- gsub("[^a-zA-Z0-9]", "_", text)
  col <- gsub("^_+|_+$", "", col)   # strip leading and trailing underscores
  paste0(prefix, col)
}

build_mode_metrics <- function(df) {
  # For every variable in analysis_vars, create a binary indicator column for
  # each unique value and record it in the metrics list.
  # 'Bike/Scooter Access or Egress (combined)' is appended at the end.
  metrics <- list()

  for (label in names(analysis_vars)) {
    data_col <- analysis_vars[[label]]
    for (mode in sort(unique(na.omit(df[[data_col]])))) {
      col_name <- safe_col(paste0(tolower(label), "__"), mode)
      df[[col_name]] <- as.integer(df[[data_col]] == mode)
      metrics[[paste0(label, ": ", mode)]] <- col_name
    }
  }

  df[["bike_any"]] <- as.integer(
    df[["ORIGIN_TRANSPORT"]] %in% bike_scooter_modes |
    df[["DESTIN_TRANSPORT"]] %in% bike_scooter_modes
  )
  metrics[["Bike/Scooter Access or Egress (combined)"]] <- "bike_any"

  list(df = df, metrics = metrics)
}

estimate_share <- function(svy_design, df, col) {
  # Weighted mean share + Wald CI for a binary 0/1 column.
  # Uses survey_mean() (Horvitz-Thompson estimator).
  # CI bounds use the normal approximation (z = 1.96) to match the Python script.
  result <- svy_design |>
    summarise(
      share = survey_mean(!!sym(col), vartype = c("se", "cv"), na.rm = TRUE),
      wgt   = survey_total(!!sym(col), vartype = "se", na.rm = TRUE)
    )

  est <- result$share
  se  <- result$share_se
  lci <- est - Z_VALUE * se
  uci <- est + Z_VALUE * se

  list(
    weighted_share   = est,
    se               = se,
    ci_lower_95      = lci,
    ci_upper_95      = uci,
    ci_95            = Z_VALUE * se,
    coeff_of_var     = result$share_cv,
    weighted_count   = result$wgt,
    total_weighted   = sum(df$ADJ_WGHT_FCTR, na.rm = TRUE),
    unweighted_count = as.integer(sum(df[[col]], na.rm = TRUE)),
    total_unweighted = nrow(df)
  )
}

reliability_label <- function(cv_flag, sample_size_flag, ci_width_flag) {
  case_when(
    cv_flag          ~ "Poor (High CV >30%)",
    sample_size_flag ~ "Poor (Small sample n<30)",
    ci_width_flag    ~ "Poor (Wide CI >40pp)",
    TRUE             ~ "Acceptable"
  )
}

run_analysis <- function(df_full, filter_label, filter_col, filter_pattern, ignore_case) {
  cat(glue("\n{strrep('=', 60)}\n"))
  cat(glue("Route filter: {filter_label}\n"))

  # ── Filter rows ───────────────────────────────────────────
  df <- df_full[grepl(filter_pattern, df_full[[filter_col]],
                      ignore.case = ignore_case, perl = TRUE), ]
  cat(glue("Records after filter: {nrow(df)}\n"))

  # ── Matched routes summary ────────────────────────────────
  routes_file <- sub(
    "\\.csv$",
    paste0("_matched_routes_", gsub("/", "-", filter_label), ".csv"),
    output_file
  )
  matched <- df |>
    group_by(`ROUTE_DIRECTION[Code]`, ROUTE_DIRECTION) |>
    summarise(n_records = n(), .groups = "drop") |>
    arrange(ROUTE_DIRECTION)
  write.csv(matched, routes_file, row.names = FALSE)
  cat(glue("Matched routes written to {routes_file}\n"))
  print(as.data.frame(matched))

  # ── Strata & drop incomplete rows ─────────────────────────
  df <- df |>
    mutate(stratum = paste(ROUTE_DIRECTION, TIME_PERIOD, DATE_TYPE, sep = " | "))
  n_before <- nrow(df)
  df <- df |> filter(!is.na(TIME_PERIOD), !is.na(DATE_TYPE), !is.na(LINKED_WGHT_FCTR))
  cat(glue("Dropped {n_before - nrow(df)} rows with NA in strata/weight; {nrow(df)} remaining\n"))

  # ── Average-day weight adjustment ────────────────────────
  # Scale the survey weight so each record represents one average day:
  #   weekday records × 5  (Mon–Fri)
  #   weekend records × 2  (Sat–Sun)
  #   divide by 7          (days in a week)
  df <- df |>
    mutate(ADJ_WGHT_FCTR = LINKED_WGHT_FCTR * day_weights[DATE_TYPE] / 7)

  # ── Build metrics (adds binary columns to df) ─────────────
  built   <- build_mode_metrics(df)
  df      <- built$df
  metrics <- built$metrics

  # ── Survey design ─────────────────────────────────────────
  # No PSU: vehicle ID not available.
  # Singleton handling is controlled by the global options(survey.lonely.psu)
  # set at the top of this script.
  svy_design <- df |>
    as_survey_design(strata = stratum, weights = ADJ_WGHT_FCTR)

  # ── Estimate shares ───────────────────────────────────────
  results_list <- lapply(names(metrics), function(metric_label) {
    col <- metrics[[metric_label]]
    row <- estimate_share(svy_design, df, col)
    row$metric       <- metric_label
    row$route_filter <- filter_label
    as.data.frame(row)
  })

  results_df <- bind_rows(results_list)

  # ── Reliability flags ─────────────────────────────────────
  results_df <- results_df |>
    mutate(
      cv_flag          = coeff_of_var > 0.30,
      sample_size_flag = total_unweighted < 30,
      ci_width_flag    = (ci_upper_95 - ci_lower_95) > 0.40,
      suppress         = cv_flag | sample_size_flag | ci_width_flag,
      estimate_reliability = reliability_label(cv_flag, sample_size_flag, ci_width_flag)
    )

  # ── Print summary ─────────────────────────────────────────
  cat(glue("\n── Results: {filter_label} ──\n\n"))
  for (i in seq_len(nrow(results_df))) {
    cat(sprintf(
      "  %-45s  %5.1f%%  (95%% CI: %5.1f%% \u2013 %5.1f%%)  [n=%d]  %s\n",
      results_df$metric[i],
      results_df$weighted_share[i] * 100,
      results_df$ci_lower_95[i]    * 100,
      results_df$ci_upper_95[i]    * 100,
      results_df$unweighted_count[i],
      results_df$estimate_reliability[i]
    ))
  }

  results_df
}

# ── Main ─────────────────────────────────────────────────────
df_full <- read_excel(file_path, sheet = "OD_RESULTS")
cat(glue("Loaded {nrow(df_full)} rows from OD_RESULTS\n"))

all_results <- lapply(route_filters, function(f) {
  run_analysis(df_full, f$label, f$col, f$pattern, f$ignore_case)
})

combined <- bind_rows(all_results)

col_order <- c(
  "route_filter", "metric", "weighted_share", "se", "ci_95",
  "ci_lower_95", "ci_upper_95", "coeff_of_var",
  "weighted_count", "unweighted_count", "total_weighted",
  "total_unweighted", "estimate_reliability"
)
combined <- combined[, col_order]

write.csv(combined, output_file, row.names = FALSE)
cat(glue("\nWrote {nrow(combined)} rows to {output_file}\n"))
