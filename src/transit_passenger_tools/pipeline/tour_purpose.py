"""Transform tour purpose field.

This module derives tour purpose from origin/destination purposes and work/school status.
Implements R Build_Standard_Database.R Lines 864-970 logic.
"""

import polars as pl

from transit_passenger_tools.codebook import (
    StudentStatus,
    TourPurpose,
    TripPurpose,
    WorkStatus,
)

# Age thresholds for school categorization
AGE_GRADE_SCHOOL = 14
AGE_COLLEGE = 18


def derive_tour_purpose(df: pl.DataFrame) -> pl.DataFrame:
    """Calculate tour purpose from trip characteristics.

    Implements complex case_when logic from R Lines 864-970 to determine
    the primary purpose of the tour (multi-stop trip chain).

    Logic priority (42 conditions total):
    1. Work tours (home-work, work-home)
    2. School tours by age (grade school, high school, university)
    3. Non-worker specific tours
    4. At-work sub-tours
    5. Worker/missing status default tours
    6. Fallback to origin purpose

    Args:
        df: DataFrame with orig_purp, dest_purp, work_status, student_status,
            at_work/at_school flags, approximate_age

    Returns:
        DataFrame with tour_purp and tour_purp_case columns added
        (returns unchanged if required columns missing)
    """
    # Check if required columns exist; if not, return unchanged
    # This follows validation requirement: "required_together: orig_purp, dest_purp"
    required = [
        "orig_purp",
        "dest_purp",
        "work_status",
        "student_status",
        "approximate_age",
        "at_work_prior_to_orig_purp",
        "at_work_after_dest_purp",
        "at_school_prior_to_orig_purp",
        "at_school_after_dest_purp",
    ]
    missing = [col for col in required if col not in df.columns]
    if missing:
        # Cannot derive tour_purp without all required columns - return unchanged
        return df

    # First, standardize school purposes by age
    # School -> high school (default)
    # School + age < 14 -> grade school
    # School + age > 18 -> college (for trip_purp only)

    result_df = df.with_columns([
        # Standardize orig_purp
        pl.when(pl.col("orig_purp") == TripPurpose.SCHOOL.value)
        .then(
            pl.when(pl.col("approximate_age") < AGE_GRADE_SCHOOL)
            .then(pl.lit(TripPurpose.GRADE_SCHOOL.value))
            .otherwise(pl.lit(TripPurpose.HIGH_SCHOOL.value))
        )
        .otherwise(pl.col("orig_purp"))
        .alias("orig_purp"),

        # Standardize dest_purp
        pl.when(pl.col("dest_purp") == TripPurpose.SCHOOL.value)
        .then(
            pl.when(pl.col("approximate_age") < AGE_GRADE_SCHOOL)
            .then(pl.lit(TripPurpose.GRADE_SCHOOL.value))
            .otherwise(pl.lit(TripPurpose.HIGH_SCHOOL.value))
        )
        .otherwise(pl.col("dest_purp"))
        .alias("dest_purp"),
    ])

    # For trip_purp if it exists
    if "trip_purp" in df.columns:
        result_df = result_df.with_columns([
            pl.when(pl.col("trip_purp") == TripPurpose.SCHOOL.value)
            .then(
                pl.when(pl.col("approximate_age") < AGE_GRADE_SCHOOL)
                .then(pl.lit(TripPurpose.GRADE_SCHOOL.value))
                .when(pl.col("approximate_age") > AGE_COLLEGE)
                .then(pl.lit(TripPurpose.UNIVERSITY.value))
                .otherwise(pl.lit(TripPurpose.HIGH_SCHOOL.value))
            )
            .otherwise(pl.col("trip_purp"))
            .alias("trip_purp")
        ])

    # Build the complex tour purpose logic
    # Returns both tour_purp and tour_purp_case concatenated with "___" separator

    tour_expr = (
        # 1. Work, Home to Work
        pl.when(
            (pl.col("orig_purp") == TripPurpose.HOME.value) &
            (pl.col("dest_purp") == TripPurpose.WORK.value)
        )
        .then(pl.lit(f"{TourPurpose.WORK.value}___home to work"))

        # 2. Work, Work to Home
        .when(
            (pl.col("orig_purp") == TripPurpose.WORK.value) &
            (pl.col("dest_purp") == TripPurpose.HOME.value)
        )
        .then(pl.lit(f"{TourPurpose.WORK.value}___work to home"))

        # 3. Grade school
        .when(
            (pl.col("orig_purp") == TripPurpose.GRADE_SCHOOL.value) |
            (pl.col("dest_purp") == TripPurpose.GRADE_SCHOOL.value)
        )
        .then(pl.lit(f"{TourPurpose.GRADE_SCHOOL.value}___grade school o or d"))

        # 4. High school
        .when(
            (pl.col("orig_purp") == TripPurpose.HIGH_SCHOOL.value) |
            (pl.col("dest_purp") == TripPurpose.HIGH_SCHOOL.value)
        )
        .then(pl.lit(f"{TourPurpose.HIGH_SCHOOL.value}___high school o or d"))

        # 5. Non-worker university origin or destination
        .when(
            (pl.col("work_status") == WorkStatus.NON_WORKER.value) &
            ((pl.col("orig_purp") == TripPurpose.UNIVERSITY.value) | (pl.col("dest_purp") == TripPurpose.UNIVERSITY.value))
        )
        .then(pl.lit(f"{TourPurpose.UNIVERSITY.value}___non-worker university o or d"))

        # 6. Home to destination, non-worker non-student
        .when(
            (pl.col("work_status") == WorkStatus.NON_WORKER.value) &
            (pl.col("student_status") == StudentStatus.NON_STUDENT.value) &
            (pl.col("orig_purp") == TripPurpose.HOME.value)
        )
        .then(pl.col("dest_purp") + pl.lit("___home to destination nw"))

        # 7. Origin to home, non-worker non-student
        .when(
            (pl.col("work_status") == WorkStatus.NON_WORKER.value) &
            (pl.col("student_status") == StudentStatus.NON_STUDENT.value) &
            (pl.col("dest_purp") == TripPurpose.HOME.value)
        )
        .then(pl.col("orig_purp") + pl.lit("___origin to home nw"))

        # 8. Origin=destination, non-worker non-student
        .when(
            (pl.col("work_status") == WorkStatus.NON_WORKER.value) &
            (pl.col("student_status") == StudentStatus.NON_STUDENT.value) &
            (pl.col("orig_purp") == pl.col("dest_purp"))
        )
        .then(pl.col("orig_purp") + pl.lit("___orig=destination"))

        # 9. Non-home-based escorting, non-worker non-student
        .when(
            (pl.col("work_status") == WorkStatus.NON_WORKER.value) &
            (pl.col("student_status") == StudentStatus.NON_STUDENT.value) &
            ((pl.col("orig_purp") == TripPurpose.ESCORTING.value) | (pl.col("dest_purp") == TripPurpose.ESCORTING.value))
        )
        .then(pl.lit(f"{TourPurpose.ESCORTING.value}___non-home escorting o or d"))

        # 10. University present, no work before or after
        .when(
            ~pl.col("at_work_prior_to_orig_purp") &
            ~pl.col("at_work_after_dest_purp") &
            ((pl.col("orig_purp") == TripPurpose.UNIVERSITY.value) | (pl.col("dest_purp") == TripPurpose.UNIVERSITY.value))
        )
        .then(pl.lit(f"{TourPurpose.UNIVERSITY.value}___univ present, no work b+a"))

        # 11. Work before trip, home destination
        .when(
            pl.col("at_work_prior_to_orig_purp") &
            (pl.col("dest_purp") == TripPurpose.HOME.value)
        )
        .then(pl.lit(f"{TourPurpose.WORK.value}___work before, home destination"))

        # 12. Home origin, work after
        .when(
            pl.col("at_work_after_dest_purp") &
            (pl.col("orig_purp") == TripPurpose.HOME.value)
        )
        .then(pl.lit(f"{TourPurpose.WORK.value}___home origin, work after"))

        # 13. Non-worker, school before trip, home destination, >18
        .when(
            (pl.col("work_status") == WorkStatus.NON_WORKER.value) &
            pl.col("at_school_prior_to_orig_purp") &
            (pl.col("approximate_age") > AGE_COLLEGE) &
            (pl.col("dest_purp") == TripPurpose.HOME.value)
        )
        .then(pl.lit(f"{TourPurpose.UNIVERSITY.value}___non-wrkr, school b, home d"))

        # 14. Non-worker, school after trip, home origin, >18
        .when(
            (pl.col("work_status") == WorkStatus.NON_WORKER.value) &
            pl.col("at_school_after_dest_purp") &
            (pl.col("approximate_age") > AGE_COLLEGE) &
            (pl.col("orig_purp") == TripPurpose.HOME.value)
        )
        .then(pl.lit(f"{TourPurpose.UNIVERSITY.value}___non-wrkr, school a, home o"))

        # 15. Non-worker, school before trip, home destination, 14-18
        .when(
            (pl.col("work_status") == WorkStatus.NON_WORKER.value) &
            pl.col("at_school_prior_to_orig_purp") &
            (pl.col("approximate_age") <= AGE_COLLEGE) &
            (pl.col("approximate_age") >= AGE_GRADE_SCHOOL) &
            (pl.col("dest_purp") == TripPurpose.HOME.value)
        )
        .then(pl.lit(f"{TourPurpose.HIGH_SCHOOL.value}___non-wrkr, 14-18, school b, home d"))

        # 16. Non-worker, school after trip, home origin, 14-18
        .when(
            (pl.col("work_status") == WorkStatus.NON_WORKER.value) &
            pl.col("at_school_after_dest_purp") &
            (pl.col("approximate_age") <= AGE_COLLEGE) &
            (pl.col("approximate_age") >= AGE_GRADE_SCHOOL) &
            (pl.col("orig_purp") == TripPurpose.HOME.value)
        )
        .then(pl.lit(f"{TourPurpose.HIGH_SCHOOL.value}___non-wrkr, 14-18, school a, home o"))

        # 17. Work origin or destination, not before or after
        .when(
            ~pl.col("at_work_prior_to_orig_purp") &
            ~pl.col("at_work_after_dest_purp") &
            ((pl.col("orig_purp") == TripPurpose.WORK.value) | (pl.col("dest_purp") == TripPurpose.WORK.value))
        )
        .then(pl.lit(f"{TourPurpose.WORK.value}___work o or d"))

        # 18. At work subtour: work before and work destination
        .when(
            pl.col("at_work_prior_to_orig_purp") &
            (pl.col("dest_purp") == TripPurpose.WORK.value)
        )
        .then(pl.lit(f"{TourPurpose.AT_WORK.value}___at work subtour work b, work d"))

        # 19. At work subtour: work after and work origin
        .when(
            pl.col("at_work_after_dest_purp") &
            (pl.col("orig_purp") == TripPurpose.WORK.value)
        )
        .then(pl.lit(f"{TourPurpose.AT_WORK.value}___at work subtour work a, work o"))

        # 20. At work subtour: work before and work after
        .when(
            pl.col("at_work_after_dest_purp") &
            pl.col("at_work_prior_to_orig_purp")
        )
        .then(pl.lit(f"{TourPurpose.WORK.value}___at work subtour work a, work b"))

        # 21. Home to destination, worker or missing information
        .when(pl.col("orig_purp") == TripPurpose.HOME.value)
        .then(pl.col("dest_purp") + pl.lit("___home to destination w"))

        # 22. Origin to home, worker or missing information
        .when(pl.col("dest_purp") == TripPurpose.HOME.value)
        .then(pl.col("orig_purp") + pl.lit("___origin to home w"))

        # 23. Default: use origin purpose
        .otherwise(pl.col("orig_purp") + pl.lit("___default origin"))
    )

    result_df = result_df.with_columns([
        tour_expr.alias("_temp_tour")
    ])

    # Split temp_tour into tour_purp and tour_purp_case
    result_df = result_df.with_columns([
        pl.col("_temp_tour").str.split("___").list.get(0).alias("tour_purp"),
        pl.col("_temp_tour").str.split("___").list.get(1).alias("tour_purp_case"),
    ])

    # For surveys with trip_purp instead of orig/dest_purp
    if "trip_purp" in df.columns:
        result_df = result_df.with_columns([
            pl.when(
                (pl.col("tour_purp").is_null()) &
                (pl.col("trip_purp").is_not_null())
            )
            .then(pl.col("trip_purp"))
            .otherwise(pl.col("tour_purp"))
            .alias("tour_purp"),

            pl.when(
                (pl.col("tour_purp").is_null()) &
                (pl.col("trip_purp").is_not_null())
            )
            .then(pl.lit("trip_purp"))
            .otherwise(pl.col("tour_purp_case"))
            .alias("tour_purp_case"),
        ])

    # Finally, recode work-related and business apt as 'other maintenance'
    result_df = result_df.with_columns([
        pl.when(pl.col("tour_purp").is_in([TripPurpose.WORK_RELATED.value, "business apt"]))
        .then(pl.lit(TourPurpose.OTHER_MAINTENANCE.value))
        .otherwise(pl.col("tour_purp"))
        .alias("tour_purp")
    ])

    # Drop temp column
    result_df = result_df.drop("_temp_tour")

    return result_df
