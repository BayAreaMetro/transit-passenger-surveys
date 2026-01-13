"""Initial schema with SurveyResponse and SurveyWeight tables

Revision ID: b3a843df7ad9
Revises: 
Create Date: 2026-01-13 10:56:38.759750

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'b3a843df7ad9'
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Create survey_responses table
    op.create_table(
        'survey_responses',
        sa.Column('unique_id', sa.String(100), primary_key=True, nullable=False),
        sa.Column('id', sa.String(50), nullable=False),
        sa.Column('operator', sa.String(50), nullable=False),
        sa.Column('survey_year', sa.Integer(), nullable=False),
        sa.Column('canonical_operator', sa.String(50), nullable=True),
        sa.Column('survey_name', sa.String(100), nullable=True),
        
        # Trip Information
        sa.Column('route', sa.String(50), nullable=True),
        sa.Column('direction', sa.Integer(), nullable=True),
        sa.Column('boardings', sa.Integer(), nullable=True),
        
        # Access/Egress Modes
        sa.Column('access_mode', sa.Integer(), nullable=True),
        sa.Column('egress_mode', sa.Integer(), nullable=True),
        sa.Column('immediate_access_mode', sa.Integer(), nullable=True),
        sa.Column('immediate_egress_mode', sa.Integer(), nullable=True),
        
        # Transfer Information
        sa.Column('transfer_from', sa.String(50), nullable=True),
        sa.Column('transfer_to', sa.String(50), nullable=True),
        sa.Column('number_transfers_orig_board', sa.Integer(), nullable=True),
        sa.Column('number_transfers_alight_dest', sa.Integer(), nullable=True),
        sa.Column('first_route_before_survey_board', sa.String(50), nullable=True),
        sa.Column('second_route_before_survey_board', sa.String(50), nullable=True),
        sa.Column('third_route_before_survey_board', sa.String(50), nullable=True),
        sa.Column('first_route_after_survey_alight', sa.String(50), nullable=True),
        sa.Column('second_route_after_survey_alight', sa.String(50), nullable=True),
        sa.Column('third_route_after_survey_alight', sa.String(50), nullable=True),
        
        # Technology/Path
        sa.Column('survey_tech', sa.String(50), nullable=True),
        sa.Column('first_board_tech', sa.String(50), nullable=True),
        sa.Column('last_alight_tech', sa.String(50), nullable=True),
        sa.Column('path_access', sa.String(20), nullable=True),
        sa.Column('path_egress', sa.String(20), nullable=True),
        sa.Column('path_line_haul', sa.String(20), nullable=True),
        sa.Column('path_label', sa.String(50), nullable=True),
        
        # Trip Purpose
        sa.Column('orig_purp', sa.Integer(), nullable=True),
        sa.Column('dest_purp', sa.Integer(), nullable=True),
        sa.Column('tour_purp', sa.Integer(), nullable=True),
        sa.Column('trip_purp', sa.Integer(), nullable=True),
        
        # Ancillary Variables
        sa.Column('at_work_prior_to_orig_purp', sa.String(50), nullable=True),
        sa.Column('at_work_after_dest_purp', sa.String(50), nullable=True),
        sa.Column('at_school_prior_to_orig_purp', sa.String(50), nullable=True),
        sa.Column('at_school_after_dest_purp', sa.String(50), nullable=True),
        
        # Geographic - Lat/Lon
        sa.Column('orig_lat', sa.Float(), nullable=True),
        sa.Column('orig_lon', sa.Float(), nullable=True),
        sa.Column('dest_lat', sa.Float(), nullable=True),
        sa.Column('dest_lon', sa.Float(), nullable=True),
        sa.Column('home_lat', sa.Float(), nullable=True),
        sa.Column('home_lon', sa.Float(), nullable=True),
        sa.Column('workplace_lat', sa.Float(), nullable=True),
        sa.Column('workplace_lon', sa.Float(), nullable=True),
        sa.Column('school_lat', sa.Float(), nullable=True),
        sa.Column('school_lon', sa.Float(), nullable=True),
        sa.Column('first_board_lat', sa.Float(), nullable=True),
        sa.Column('first_board_lon', sa.Float(), nullable=True),
        sa.Column('last_alight_lat', sa.Float(), nullable=True),
        sa.Column('last_alight_lon', sa.Float(), nullable=True),
        sa.Column('survey_board_lat', sa.Float(), nullable=True),
        sa.Column('survey_board_lon', sa.Float(), nullable=True),
        sa.Column('survey_alight_lat', sa.Float(), nullable=True),
        sa.Column('survey_alight_lon', sa.Float(), nullable=True),
        
        # Geographic - Rail Stations
        sa.Column('onoff_enter_station', sa.String(100), nullable=True),
        sa.Column('onoff_exit_station', sa.String(100), nullable=True),
        
        # Geographic - Travel Model Zones
        sa.Column('orig_maz', sa.Integer(), nullable=True),
        sa.Column('dest_maz', sa.Integer(), nullable=True),
        sa.Column('home_maz', sa.Integer(), nullable=True),
        sa.Column('workplace_maz', sa.Integer(), nullable=True),
        sa.Column('school_maz', sa.Integer(), nullable=True),
        sa.Column('orig_taz', sa.Integer(), nullable=True),
        sa.Column('dest_taz', sa.Integer(), nullable=True),
        sa.Column('home_taz', sa.Integer(), nullable=True),
        sa.Column('workplace_taz', sa.Integer(), nullable=True),
        sa.Column('school_taz', sa.Integer(), nullable=True),
        sa.Column('first_board_tap', sa.Integer(), nullable=True),
        sa.Column('last_alight_tap', sa.Integer(), nullable=True),
        
        # Geographic - Counties
        sa.Column('home_county', sa.String(50), nullable=True),
        sa.Column('workplace_county', sa.String(50), nullable=True),
        sa.Column('school_county', sa.String(50), nullable=True),
        
        # Fare Information
        sa.Column('fare_medium', sa.Integer(), nullable=True),
        sa.Column('fare_category', sa.Integer(), nullable=True),
        sa.Column('clipper_detail', sa.Integer(), nullable=True),
        
        # Time Information
        sa.Column('depart_hour', sa.Integer(), nullable=True),
        sa.Column('return_hour', sa.Integer(), nullable=True),
        sa.Column('survey_time', sa.String(20), nullable=True),
        sa.Column('weekpart', sa.Integer(), nullable=True),
        sa.Column('day_of_the_week', sa.String(20), nullable=True),
        sa.Column('day_part', sa.String(20), nullable=True),
        sa.Column('field_start', sa.Date(), nullable=True),
        sa.Column('field_end', sa.Date(), nullable=True),
        sa.Column('date_string', sa.String(50), nullable=True),
        sa.Column('time_string', sa.String(50), nullable=True),
        
        # Person Demographics
        sa.Column('approximate_age', sa.Integer(), nullable=True),
        sa.Column('year_born_four_digit', sa.Integer(), nullable=True),
        sa.Column('gender', sa.Integer(), nullable=True),
        sa.Column('hispanic', sa.Integer(), nullable=True),
        sa.Column('race', sa.String(100), nullable=True),
        sa.Column('race_dmy_ind', sa.Integer(), nullable=True),
        sa.Column('race_dmy_asn', sa.Integer(), nullable=True),
        sa.Column('race_dmy_blk', sa.Integer(), nullable=True),
        sa.Column('race_dmy_hwi', sa.Integer(), nullable=True),
        sa.Column('race_dmy_wht', sa.Integer(), nullable=True),
        sa.Column('race_other_string', sa.String(100), nullable=True),
        sa.Column('work_status', sa.Integer(), nullable=True),
        sa.Column('student_status', sa.Integer(), nullable=True),
        sa.Column('eng_proficient', sa.Integer(), nullable=True),
        
        # Household Demographics
        sa.Column('persons', sa.Integer(), nullable=True),
        sa.Column('workers', sa.Integer(), nullable=True),
        sa.Column('vehicles', sa.Integer(), nullable=True),
        sa.Column('household_income', sa.Integer(), nullable=True),
        sa.Column('auto_suff', sa.String(50), nullable=True),
        sa.Column('language_at_home', sa.String(100), nullable=True),
        sa.Column('language_at_home_binary', sa.Integer(), nullable=True),
        sa.Column('language_at_home_detail', sa.String(100), nullable=True),
        
        # Survey Administration
        sa.Column('survey_type', sa.Integer(), nullable=True),
        sa.Column('interview_language', sa.Integer(), nullable=True),
        sa.Column('field_language', sa.Integer(), nullable=True),
        
        # Weights
        sa.Column('weight', sa.Float(), nullable=True),
        sa.Column('trip_weight', sa.Float(), nullable=True),
        
        # System Fields
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
    )
    
    # Create indices for common queries
    op.create_index('idx_operator_year', 'survey_responses', ['operator', 'survey_year'])
    op.create_index('idx_canonical_operator', 'survey_responses', ['canonical_operator'])
    op.create_index('idx_survey_year', 'survey_responses', ['survey_year'])
    op.create_index('idx_route', 'survey_responses', ['route'])
    op.create_index('idx_orig_taz', 'survey_responses', ['orig_taz'])
    op.create_index('idx_dest_taz', 'survey_responses', ['dest_taz'])
    
    # Create survey_weights table
    op.create_table(
        'survey_weights',
        sa.Column('id', sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column('unique_id', sa.String(100), nullable=False),
        sa.Column('weight_scheme', sa.String(50), nullable=False),
        sa.Column('board_weight', sa.Float(), nullable=True),
        sa.Column('trip_weight', sa.Float(), nullable=True),
        sa.Column('expansion_factor', sa.Float(), nullable=True),
        sa.Column('description', sa.String(500), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(['unique_id'], ['survey_responses.unique_id'], ),
    )
    
    # Create indices for survey_weights
    op.create_index('idx_weight_unique_id', 'survey_weights', ['unique_id'])
    op.create_index('idx_weight_scheme', 'survey_weights', ['weight_scheme'])
    op.create_index('idx_weight_unique_scheme', 'survey_weights', ['unique_id', 'weight_scheme'], unique=True)


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index('idx_weight_unique_scheme', table_name='survey_weights')
    op.drop_index('idx_weight_scheme', table_name='survey_weights')
    op.drop_index('idx_weight_unique_id', table_name='survey_weights')
    op.drop_table('survey_weights')
    
    op.drop_index('idx_dest_taz', table_name='survey_responses')
    op.drop_index('idx_orig_taz', table_name='survey_responses')
    op.drop_index('idx_route', table_name='survey_responses')
    op.drop_index('idx_survey_year', table_name='survey_responses')
    op.drop_index('idx_canonical_operator', table_name='survey_responses')
    op.drop_index('idx_operator_year', table_name='survey_responses')
    op.drop_table('survey_responses')
