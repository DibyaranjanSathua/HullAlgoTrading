"""Change date to datetime

Revision ID: 808e3e562075
Revises: 994e278b4b12
Create Date: 2022-06-27 00:20:21.896341

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '808e3e562075'
down_revision = '994e278b4b12'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('nifty_day_data', sa.Column('datetime', sa.DateTime(), nullable=True))
    op.drop_index('ix_nifty_day_data_date', table_name='nifty_day_data')
    op.create_index(op.f('ix_nifty_day_data_datetime'), 'nifty_day_data', ['datetime'], unique=True)
    op.drop_column('nifty_day_data', 'date')
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('nifty_day_data', sa.Column('date', sa.DATE(), autoincrement=False, nullable=True))
    op.drop_index(op.f('ix_nifty_day_data_datetime'), table_name='nifty_day_data')
    op.create_index('ix_nifty_day_data_date', 'nifty_day_data', ['date'], unique=False)
    op.drop_column('nifty_day_data', 'datetime')
    # ### end Alembic commands ###
