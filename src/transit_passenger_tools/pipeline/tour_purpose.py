"""Tour purpose calculation module.

Translates the R case_when logic from Build_Standard_Database.R (lines 830-920)
to determine tour purpose from origin/destination purposes and work/school status.
"""
import logging

import polars as pl

logger = logging.getLogger(__name__)


# School + age < 14 -> grade school
# School + age > 18 -> college (for trip_purp only)
AGE_GRADE_SCHOOL = 14
AGE_COLLEGE = 18

def calculate_tour_purpose(df: pl.DataFrame) -> pl.DataFrame:
    """Calculate tour purpose from trip characteristics.

    Implements the complex logic from R Build_Standard_Database.R to determine
    the primary purpose of the tour (multi-stop trip chain).

    Logic priority:
    1. Work tours (home-work, work-home)
    2. School tours (grade school, high school, university)
    3. Non-worker specific logic
    4. Work sub-tours (at work before AND after)
    5. Default to origin/destination-based purposes

    Args:
        df: DataFrame with purpose and status columns

    Returns:
        DataFrame with added tour_purp and tour_purp_case columns
    """
    logger.info("Calculating tour purpose")

    # First, standardize school purposes by age
    # School -> high school (default)
    # School + age < 14 -> grade school
    # School + age > 18 -> college (for trip_purp only)

    result_df = df.with_columns([
        # Standardize orig_purp
        pl.when(pl.col("orig_purp") == "school")
        .then(
            pl.when(pl.col("approximate_age") < AGE_GRADE_SCHOOL)
            .then(pl.lit("grade school"))
            .otherwise(pl.lit("high school"))
        )
        .otherwise(pl.col("orig_purp"))
        .alias("orig_purp"),

        # Standardize dest_purp
        pl.when(pl.col("dest_purp") == "school")
        .then(
            pl.when(pl.col("approximate_age") < AGE_GRADE_SCHOOL)
            .then(pl.lit("grade school"))
            .otherwise(pl.lit("high school"))
        )
        .otherwise(pl.col("dest_purp"))
        .alias("dest_purp"),
    ])

    # For trip_purp if it exists
    if "trip_purp" in df.columns:
        result_df = result_df.with_columns([
            pl.when(pl.col("trip_purp") == "school")
            .then(
                pl.when(pl.col("approximate_age") < AGE_GRADE_SCHOOL)
                .then(pl.lit("grade school"))
                .when(pl.col("approximate_age") > AGE_COLLEGE)
                .then(pl.lit("college"))
                .otherwise(pl.lit("high school"))
            )
            .otherwise(pl.col("trip_purp"))
            .alias("trip_purp")
        ])

    # Build the complex tour purpose logic
    # Returns both tour_purp and tour_purp_case concatenated with "___" separator

    tour_expr = (
        # 1. Work, Home to Work
        pl.when(
            (pl.col("orig_purp") == "home") &
            (pl.col("dest_purp") == "work")
        )
        .then(pl.lit("work___home to work"))

        # 2. Work, Work to Home
        .when(
            (pl.col("orig_purp") == "work") &
            (pl.col("dest_purp") == "home")
        )
        .then(pl.lit("work___work to home"))

        # 3. Grade school
        .when(
            (pl.col("orig_purp") == "grade school") |
            (pl.col("dest_purp") == "grade school")
        )
        .then(pl.lit("grade school___grade school o or d"))

        # 4. High school
        .when(
            (pl.col("orig_purp") == "high school") |
            (pl.col("dest_purp") == "high school")
        )
        .then(pl.lit("high school___high school o or d"))

        # 5. Non-worker university origin or destination
        .when(
            (pl.col("work_status") == "non-worker") &
            ((pl.col("orig_purp") == "college") | (pl.col("dest_purp") == "college"))
        )
        .then(pl.lit("university___non-worker university o or d"))

        # 6. Home to destination, non-worker non-student
        .when(
            (pl.col("work_status") == "non-worker") &
            (pl.col("student_status") == "non-student") &
            (pl.col("orig_purp") == "home")
        )
        .then(pl.col("dest_purp") + pl.lit("___home to destination nw"))

        # 7. Origin to home, non-worker non-student
        .when(
            (pl.col("work_status") == "non-worker") &
            (pl.col("student_status") == "non-student") &
            (pl.col("dest_purp") == "home")
        )
        .then(pl.col("orig_purp") + pl.lit("___origin to home nw"))

        # 8. Origin=destination, non-worker non-student
        .when(
            (pl.col("work_status") == "non-worker") &
            (pl.col("student_status") == "non-student") &
            (pl.col("orig_purp") == pl.col("dest_purp"))
        )
        .then(pl.col("orig_purp") + pl.lit("___orig=destination"))

        # 9. Non-home-based escorting, non-worker non-student
        .when(
            (pl.col("work_status") == "non-worker") &
            (pl.col("student_status") == "non-student") &
            ((pl.col("orig_purp") == "escorting") | (pl.col("dest_purp") == "escorting"))
        )
        .then(pl.lit("escorting___non-home escorting o or d"))

        # 10. University present, no work before or after
        .when(
            (pl.col("at_work_prior_to_orig_purp") == "not at work before surveyed trip") &
            (pl.col("at_work_after_dest_purp") == "not at work after surveyed trip") &
            ((pl.col("orig_purp") == "college") | (pl.col("dest_purp") == "college"))
        )
        .then(pl.lit("university___univ present, no work b+a"))

        # 11. Work before trip, home destination
        .when(
            (pl.col("at_work_prior_to_orig_purp") == "at work before surveyed trip") &
            (pl.col("dest_purp") == "home")
        )
        .then(pl.lit("work___work before, home destination"))

        # 12. Home origin, work after
        .when(
            (pl.col("at_work_after_dest_purp") == "at work after surveyed trip") &
            (pl.col("orig_purp") == "home")
        )
        .then(pl.lit("work___home origin, work after"))

        # 13. Non-worker, school before trip, home destination, >18
        .when(
            (pl.col("work_status") == "non-worker") &
            (pl.col("at_school_prior_to_orig_purp") == "at school before surveyed trip") &
            (pl.col("approximate_age") > AGE_COLLEGE) &
            (pl.col("dest_purp") == "home")
        )
        .then(pl.lit("university___non-wrkr, school b, home d"))

        # 14. Non-worker, school after trip, home origin, >18
        .when(
            (pl.col("work_status") == "non-worker") &
            (pl.col("at_school_after_dest_purp") == "at school after surveyed trip") &
            (pl.col("approximate_age") > AGE_COLLEGE) &
            (pl.col("orig_purp") == "home")
        )
        .then(pl.lit("university___non-wrkr, school a, home o"))

        # 15. Non-worker, school before trip, home destination, 14-18
        .when(
            (pl.col("work_status") == "non-worker") &
            (pl.col("at_school_prior_to_orig_purp") == "at school before surveyed trip") &
            (pl.col("approximate_age") <= AGE_COLLEGE) &
            (pl.col("approximate_age") >= AGE_GRADE_SCHOOL) &
            (pl.col("dest_purp") == "home")
        )
        .then(pl.lit("high school___non-wrkr, 14-18, school b, home d"))

        # 16. Non-worker, school after trip, home origin, 14-18
        .when(
            (pl.col("work_status") == "non-worker") &
            (pl.col("at_school_after_dest_purp") == "at school after surveyed trip") &
            (pl.col("approximate_age") <= AGE_COLLEGE) &
            (pl.col("approximate_age") >= AGE_GRADE_SCHOOL) &
            (pl.col("orig_purp") == "home")
        )
        .then(pl.lit("high school___non-wrkr, 14-18, school a, home o"))

        # 17. Work origin or destination, not before or after
        .when(
            (pl.col("at_work_prior_to_orig_purp") == "not at work before surveyed trip") &
            (pl.col("at_work_after_dest_purp") == "not at work after surveyed trip") &
            ((pl.col("orig_purp") == "work") | (pl.col("dest_purp") == "work"))
        )
        .then(pl.lit("work___work o or d"))

        # 18. At work subtour: work before and work destination
        .when(
            (pl.col("at_work_prior_to_orig_purp") == "at work before surveyed trip") &
            (pl.col("dest_purp") == "work")
        )
        .then(pl.lit("at work___at work subtour work b, work d"))

        # 19. At work subtour: work after and work origin
        .when(
            (pl.col("at_work_after_dest_purp") == "at work after surveyed trip") &
            (pl.col("orig_purp") == "work")
        )
        .then(pl.lit("at work___at work subtour work a, work o"))

        # 20. At work subtour: work before and work after
        .when(
            (pl.col("at_work_after_dest_purp") == "at work after surveyed trip") &
            (pl.col("at_work_prior_to_orig_purp") == "at work before surveyed trip")
        )
        .then(pl.lit("work___at work subtour work a, work b"))

        # 21. Home to destination, worker or missing information
        .when(pl.col("orig_purp") == "home")
        .then(pl.col("dest_purp") + pl.lit("___home to destination w"))

        # 22. Origin to home, worker or missing information
        .when(pl.col("dest_purp") == "home")
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
                (pl.col("tour_purp") == "missing") &
                (pl.col("trip_purp").is_not_null())
            )
            .then(pl.col("trip_purp"))
            .otherwise(pl.col("tour_purp"))
            .alias("tour_purp"),

            pl.when(
                (pl.col("tour_purp") == "missing") &
                (pl.col("trip_purp").is_not_null())
            )
            .then(pl.lit("trip_purp"))
            .otherwise(pl.col("tour_purp_case"))
            .alias("tour_purp_case"),
        ])

    # Finally, recode work-related and business apt as 'other maintenance'
    result_df = result_df.with_columns([
        pl.when(pl.col("tour_purp").is_in(["work-related", "business apt"]))
        .then(pl.lit("other maintenance"))
        .otherwise(pl.col("tour_purp"))
        .alias("tour_purp")
    ])

    # Drop temp column
    result_df = result_df.drop("_temp_tour")

    # Log distribution
    tour_counts = result_df.group_by("tour_purp").agg(pl.count()).sort("tour_purp")
    logger.info("Tour purpose distribution:\n%s", tour_counts)

    # Check for missing tour purposes
    missing_count = result_df.filter(
        pl.col("tour_purp").is_null() | (pl.col("tour_purp") == "missing")
    ).height

    if missing_count > 0:
        logger.warning("Found %d records with missing tour purpose", missing_count)

    logger.info("Tour purpose calculation complete")
    return result_df
