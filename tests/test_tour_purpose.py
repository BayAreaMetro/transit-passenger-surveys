"""Tests for tour purpose transformation module."""

import polars as pl

from transit_passenger_tools.codebook import StudentStatus, TourPurpose, TripPurpose, WorkStatus
from transit_passenger_tools.pipeline.tour_purpose import derive_tour_purpose


class TestWorkTours:
    """Test work-based tour purpose logic."""

    def test_home_to_work(self):
        """Test home to work maps to work tour."""
        df = pl.DataFrame(
            {
                "orig_purp": [TripPurpose.HOME.value],
                "dest_purp": [TripPurpose.WORK.value],
                "work_status": [WorkStatus.FULL_OR_PART_TIME.value],
                "student_status": [StudentStatus.NON_STUDENT.value],
                "at_work_prior_to_orig_purp": [False],
                "at_work_after_dest_purp": [False],
                "at_school_prior_to_orig_purp": [False],
                "at_school_after_dest_purp": [False],
                "approximate_age": [30],
            }
        )
        result = derive_tour_purpose(df)
        assert result["tour_purp"][0] == TourPurpose.WORK.value
        assert result["tour_purp_case"][0] == "home to work"

    def test_work_to_home(self):
        """Test work to home maps to work tour."""
        df = pl.DataFrame(
            {
                "orig_purp": [TripPurpose.WORK.value],
                "dest_purp": [TripPurpose.HOME.value],
                "work_status": [WorkStatus.FULL_OR_PART_TIME.value],
                "student_status": [StudentStatus.NON_STUDENT.value],
                "at_work_prior_to_orig_purp": [False],
                "at_work_after_dest_purp": [False],
                "at_school_prior_to_orig_purp": [False],
                "at_school_after_dest_purp": [False],
                "approximate_age": [30],
            }
        )
        result = derive_tour_purpose(df)
        assert result["tour_purp"][0] == TourPurpose.WORK.value
        assert result["tour_purp_case"][0] == "work to home"

    def test_work_before_home_destination(self):
        """Test at work before trip, home destination."""
        df = pl.DataFrame(
            {
                "orig_purp": [TripPurpose.SHOPPING.value],
                "dest_purp": [TripPurpose.HOME.value],
                "work_status": [WorkStatus.FULL_OR_PART_TIME.value],
                "student_status": [StudentStatus.NON_STUDENT.value],
                "at_work_prior_to_orig_purp": [True],
                "at_work_after_dest_purp": [False],
                "at_school_prior_to_orig_purp": [False],
                "at_school_after_dest_purp": [False],
                "approximate_age": [30],
            }
        )
        result = derive_tour_purpose(df)
        assert result["tour_purp"][0] == TourPurpose.WORK.value
        assert result["tour_purp_case"][0] == "work before, home destination"

    def test_home_origin_work_after(self):
        """Test home origin, at work after trip."""
        df = pl.DataFrame(
            {
                "orig_purp": [TripPurpose.HOME.value],
                "dest_purp": [TripPurpose.SHOPPING.value],
                "work_status": [WorkStatus.FULL_OR_PART_TIME.value],
                "student_status": [StudentStatus.NON_STUDENT.value],
                "at_work_prior_to_orig_purp": [False],
                "at_work_after_dest_purp": [True],
                "at_school_prior_to_orig_purp": [False],
                "at_school_after_dest_purp": [False],
                "approximate_age": [30],
            }
        )
        result = derive_tour_purpose(df)
        assert result["tour_purp"][0] == TourPurpose.WORK.value
        assert result["tour_purp_case"][0] == "home origin, work after"


class TestSchoolTours:
    """Test school-based tour purpose logic."""

    def test_grade_school_origin(self):
        """Test grade school at origin."""
        df = pl.DataFrame(
            {
                "orig_purp": [TripPurpose.GRADE_SCHOOL.value],
                "dest_purp": [TripPurpose.HOME.value],
                "work_status": [WorkStatus.NON_WORKER.value],
                "student_status": [StudentStatus.FULL_OR_PART_TIME.value],
                "at_work_prior_to_orig_purp": [False],
                "at_work_after_dest_purp": [False],
                "at_school_prior_to_orig_purp": [False],
                "at_school_after_dest_purp": [False],
                "approximate_age": [10],
            }
        )
        result = derive_tour_purpose(df)
        assert result["tour_purp"][0] == TourPurpose.GRADE_SCHOOL.value
        assert result["tour_purp_case"][0] == "grade school o or d"

    def test_high_school_destination(self):
        """Test high school at destination."""
        df = pl.DataFrame(
            {
                "orig_purp": [TripPurpose.HOME.value],
                "dest_purp": [TripPurpose.HIGH_SCHOOL.value],
                "work_status": [WorkStatus.NON_WORKER.value],
                "student_status": [StudentStatus.FULL_OR_PART_TIME.value],
                "at_work_prior_to_orig_purp": [False],
                "at_work_after_dest_purp": [False],
                "at_school_prior_to_orig_purp": [False],
                "at_school_after_dest_purp": [False],
                "approximate_age": [16],
            }
        )
        result = derive_tour_purpose(df)
        assert result["tour_purp"][0] == TourPurpose.HIGH_SCHOOL.value
        assert result["tour_purp_case"][0] == "high school o or d"

    def test_university_non_worker(self):
        """Test university tour for non-worker."""
        df = pl.DataFrame(
            {
                "orig_purp": [TripPurpose.HOME.value],
                "dest_purp": [TripPurpose.UNIVERSITY.value],
                "work_status": [WorkStatus.NON_WORKER.value],
                "student_status": [StudentStatus.FULL_OR_PART_TIME.value],
                "at_work_prior_to_orig_purp": [False],
                "at_work_after_dest_purp": [False],
                "at_school_prior_to_orig_purp": [False],
                "at_school_after_dest_purp": [False],
                "approximate_age": [20],
            }
        )
        result = derive_tour_purpose(df)
        assert result["tour_purp"][0] == TourPurpose.UNIVERSITY.value
        assert result["tour_purp_case"][0] == "non-worker university o or d"


class TestNonWorkerTours:
    """Test non-worker specific tour logic."""

    def test_non_worker_home_to_shopping(self):
        """Test non-worker non-student home to destination."""
        df = pl.DataFrame(
            {
                "orig_purp": [TripPurpose.HOME.value],
                "dest_purp": [TripPurpose.SHOPPING.value],
                "work_status": [WorkStatus.NON_WORKER.value],
                "student_status": [StudentStatus.NON_STUDENT.value],
                "at_work_prior_to_orig_purp": [False],
                "at_work_after_dest_purp": [False],
                "at_school_prior_to_orig_purp": [False],
                "at_school_after_dest_purp": [False],
                "approximate_age": [65],
            }
        )
        result = derive_tour_purpose(df)
        assert result["tour_purp"][0] == TourPurpose.SHOPPING.value
        assert result["tour_purp_case"][0] == "home to destination nw"

    def test_non_worker_medical_to_home(self):
        """Test non-worker non-student origin to home."""
        df = pl.DataFrame(
            {
                "orig_purp": [TripPurpose.OTHER_MAINTENANCE.value],
                "dest_purp": [TripPurpose.HOME.value],
                "work_status": [WorkStatus.NON_WORKER.value],
                "student_status": [StudentStatus.NON_STUDENT.value],
                "at_work_prior_to_orig_purp": [False],
                "at_work_after_dest_purp": [False],
                "at_school_prior_to_orig_purp": [False],
                "at_school_after_dest_purp": [False],
                "approximate_age": [70],
            }
        )
        result = derive_tour_purpose(df)
        # work-related gets recoded to Other maintenance
        assert result["tour_purp"][0] == TourPurpose.OTHER_MAINTENANCE.value
        assert result["tour_purp_case"][0] == "origin to home nw"

    def test_non_worker_escorting(self):
        """Test non-worker non-student escorting tour."""
        df = pl.DataFrame(
            {
                "orig_purp": [TripPurpose.SHOPPING.value],
                "dest_purp": [TripPurpose.ESCORTING.value],
                "work_status": [WorkStatus.NON_WORKER.value],
                "student_status": [StudentStatus.NON_STUDENT.value],
                "at_work_prior_to_orig_purp": [False],
                "at_work_after_dest_purp": [False],
                "at_school_prior_to_orig_purp": [False],
                "at_school_after_dest_purp": [False],
                "approximate_age": [40],
            }
        )
        result = derive_tour_purpose(df)
        assert result["tour_purp"][0] == TourPurpose.ESCORTING.value
        assert result["tour_purp_case"][0] == "non-home escorting o or d"


class TestAtWorkSubtours:
    """Test at-work subtour logic."""

    def test_at_work_subtour_before_and_after(self):
        """Test at work before AND after surveyed trip."""
        df = pl.DataFrame(
            {
                "orig_purp": [TripPurpose.WORK_RELATED.value],
                "dest_purp": [TripPurpose.EAT_OUT.value],
                "work_status": [WorkStatus.FULL_OR_PART_TIME.value],
                "student_status": [StudentStatus.NON_STUDENT.value],
                "at_work_prior_to_orig_purp": [True],
                "at_work_after_dest_purp": [True],
                "at_school_prior_to_orig_purp": [False],
                "at_school_after_dest_purp": [False],
                "approximate_age": [35],
            }
        )
        result = derive_tour_purpose(df)
        assert result["tour_purp"][0] == TourPurpose.WORK.value
        assert result["tour_purp_case"][0] == "at work subtour work a, work b"


class TestWorkerDefaultTours:
    """Test worker/missing status default tours."""

    def test_worker_home_to_shopping(self):
        """Test worker home to destination (not work)."""
        df = pl.DataFrame(
            {
                "orig_purp": [TripPurpose.HOME.value],
                "dest_purp": [TripPurpose.SHOPPING.value],
                "work_status": [WorkStatus.FULL_OR_PART_TIME.value],
                "student_status": [StudentStatus.NON_STUDENT.value],
                "at_work_prior_to_orig_purp": [False],
                "at_work_after_dest_purp": [False],
                "at_school_prior_to_orig_purp": [False],
                "at_school_after_dest_purp": [False],
                "approximate_age": [40],
            }
        )
        result = derive_tour_purpose(df)
        assert result["tour_purp"][0] == TourPurpose.SHOPPING.value
        assert result["tour_purp_case"][0] == "home to destination w"

    def test_worker_eat_out_to_home(self):
        """Test worker origin to home."""
        df = pl.DataFrame(
            {
                "orig_purp": [TripPurpose.EAT_OUT.value],
                "dest_purp": [TripPurpose.HOME.value],
                "work_status": [WorkStatus.FULL_OR_PART_TIME.value],
                "student_status": [StudentStatus.NON_STUDENT.value],
                "at_work_prior_to_orig_purp": [False],
                "at_work_after_dest_purp": [False],
                "at_school_prior_to_orig_purp": [False],
                "at_school_after_dest_purp": [False],
                "approximate_age": [40],
            }
        )
        result = derive_tour_purpose(df)
        assert result["tour_purp"][0] == TripPurpose.EAT_OUT.value
        assert result["tour_purp_case"][0] == "origin to home w"


class TestSpecialCases:
    """Test special cases and fallbacks."""

    def test_work_related_recode_to_other_maintenance(self):
        """Test work-related recoded to other maintenance."""
        df = pl.DataFrame(
            {
                "orig_purp": [TripPurpose.WORK_RELATED.value],
                "dest_purp": [TripPurpose.SHOPPING.value],
                "work_status": [WorkStatus.FULL_OR_PART_TIME.value],
                "student_status": [StudentStatus.NON_STUDENT.value],
                "at_work_prior_to_orig_purp": [False],
                "at_work_after_dest_purp": [False],
                "at_school_prior_to_orig_purp": [False],
                "at_school_after_dest_purp": [False],
                "approximate_age": [35],
            }
        )
        result = derive_tour_purpose(df)
        assert result["tour_purp"][0] == TourPurpose.OTHER_MAINTENANCE.value

    def test_default_to_origin(self):
        """Test fallback to origin purpose."""
        df = pl.DataFrame(
            {
                "orig_purp": [TripPurpose.SOCIAL_RECREATION.value],
                "dest_purp": [TripPurpose.SHOPPING.value],
                "work_status": [WorkStatus.FULL_OR_PART_TIME.value],
                "student_status": [StudentStatus.NON_STUDENT.value],
                "at_work_prior_to_orig_purp": [False],
                "at_work_after_dest_purp": [False],
                "at_school_prior_to_orig_purp": [False],
                "at_school_after_dest_purp": [False],
                "approximate_age": [25],
            }
        )
        result = derive_tour_purpose(df)
        assert result["tour_purp"][0] == TripPurpose.SOCIAL_RECREATION.value
        assert result["tour_purp_case"][0] == "default origin"
