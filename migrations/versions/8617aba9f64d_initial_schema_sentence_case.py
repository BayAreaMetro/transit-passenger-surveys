"""initial_schema_sentence_case

Revision ID: 8617aba9f64d
Revises: 3198761ab2a6
Create Date: 2026-01-13 12:16:39.457966

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '8617aba9f64d'
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create survey_responses and survey_weights tables with sentence case enums."""
    
    # Create survey_responses table
    op.execute("""
        CREATE TABLE survey_responses (
            -- Survey Identifiers (REQUIRED)
            canonical_operator VARCHAR(50) NOT NULL,
            survey_year INTEGER NOT NULL,
            survey_name VARCHAR(100) NOT NULL,
            survey_batch VARCHAR(50),
            source VARCHAR(50),
            
            -- Unique Identifiers (REQUIRED)
            id INTEGER NOT NULL,
            unique_id VARCHAR(100) NOT NULL PRIMARY KEY,
            
            -- Spatial
            route VARCHAR(50) NOT NULL,
            direction VARCHAR(50) NOT NULL,
            board_tap INTEGER,
            alight_tap INTEGER,
            orig_taz INTEGER,
            dest_taz INTEGER,
            
            -- Access/Egress (REQUIRED)
            access_mode VARCHAR(50) NOT NULL,
            egress_mode VARCHAR(50) NOT NULL,
            
            -- Transfers (REQUIRED)
            transfer_from VARCHAR(50) NOT NULL,
            transfer_to VARCHAR(50) NOT NULL,
            
            -- Technical Fields (REQUIRED)
            survey_tech VARCHAR(50) NOT NULL,
            first_board_tech VARCHAR(50) NOT NULL,
            last_alight_tech VARCHAR(50) NOT NULL,
            path_access VARCHAR(255) NOT NULL,
            path_egress VARCHAR(255) NOT NULL,
            path_line_haul VARCHAR(255) NOT NULL,
            path_label VARCHAR(255) NOT NULL,
            
            -- Trip Purpose (REQUIRED)
            orig_purp VARCHAR(50) NOT NULL,
            dest_purp VARCHAR(50) NOT NULL,
            tour_purp VARCHAR(50) NOT NULL,
            
            -- Fare (REQUIRED)
            fare_medium VARCHAR(100) NOT NULL,
            fare_category VARCHAR(100) NOT NULL,
            fare_paid REAL,
            
            -- Temporal (REQUIRED)
            weekpart VARCHAR(50) NOT NULL,
            day_of_the_week VARCHAR(50) NOT NULL,
            field_start DATE NOT NULL,
            field_end DATE NOT NULL,
            approximate_hour REAL,
            
            -- Demographics (REQUIRED)
            approximate_age INTEGER NOT NULL,
            gender VARCHAR(50) NOT NULL,
            hispanic VARCHAR(100) NOT NULL,
            race VARCHAR(100) NOT NULL,
            work_status VARCHAR(100) NOT NULL,
            student_status VARCHAR(100) NOT NULL,
            eng_proficient VARCHAR(50) NOT NULL,
            
            -- Household (REQUIRED)
            vehicles VARCHAR(50) NOT NULL,
            household_income VARCHAR(100) NOT NULL,
            hh_income_nominal_continuous REAL,
            hh_income_2023dollars_continuous REAL,
            income_lower_bound REAL,
            income_upper_bound REAL,
            language_at_home VARCHAR(100) NOT NULL,
            
            -- Weights (REQUIRED)
            weight REAL NOT NULL,
            trip_weight REAL NOT NULL,
            
            -- Survey Administration (REQUIRED)
            survey_type VARCHAR(50) NOT NULL,
            interview_language VARCHAR(50),
            field_language VARCHAR(50) NOT NULL,
            
            -- Optional Fields
            auto_suff VARCHAR(50),
            disability VARCHAR(50),
            disability_type VARCHAR(50),
            work_tour_mode VARCHAR(50),
            work_county VARCHAR(50),
            work_state VARCHAR(50),
            work_taz INTEGER,
            school_tour_mode VARCHAR(50),
            school_taz INTEGER,
            school_county VARCHAR(50),
            school_state VARCHAR(50),
            workplace_has_parking VARCHAR(50),
            free_parking_at_work VARCHAR(50),
            cost_of_parking_at_work REAL,
            monthly_parking_cost REAL,
            distance_home_to_work REAL,
            commute_frequency VARCHAR(50),
            distance_home_to_school REAL,
            own_transit_pass VARCHAR(50),
            avg_days_per_week_work REAL,
            avg_days_per_week_school REAL,
            home_county VARCHAR(50),
            home_state VARCHAR(50),
            home_taz INTEGER,
            person_type VARCHAR(50),
            transit_pass_type VARCHAR(50),
            transit_pass_subsidy VARCHAR(50),
            transit_pass_cost REAL,
            employer_subsidy REAL,
            approximate_year_of_birth INTEGER,
            household_size INTEGER,
            num_workers INTEGER
        )
    """)
    
    # Create indices for common queries
    op.create_index('idx_canonical_operator', 'survey_responses', ['canonical_operator'])
    op.create_index('idx_survey_year', 'survey_responses', ['survey_year'])
    op.create_index('idx_route', 'survey_responses', ['route'])
    op.create_index('idx_orig_taz', 'survey_responses', ['orig_taz'])
    op.create_index('idx_dest_taz', 'survey_responses', ['dest_taz'])
    op.create_index('idx_weight', 'survey_responses', ['weight'])
    op.create_index('idx_operator_year', 'survey_responses', ['canonical_operator', 'survey_year'])
    
    # Create survey_weights table
    op.execute("""
        CREATE TABLE survey_weights (
            unique_id VARCHAR(100) PRIMARY KEY,
            weight REAL NOT NULL,
            trip_weight REAL NOT NULL,
            original_weight REAL,
            original_trip_weight REAL,
            weight_method VARCHAR(100),
            weight_date DATE,
            notes TEXT,
            FOREIGN KEY (unique_id) REFERENCES survey_responses(unique_id)
        )
    """)


def downgrade() -> None:
    """Drop tables."""
    op.drop_table('survey_weights')
    op.drop_table('survey_responses')
