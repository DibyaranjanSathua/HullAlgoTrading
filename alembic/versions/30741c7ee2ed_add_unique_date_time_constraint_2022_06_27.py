"""Add unique date time constraint

Revision ID: 30741c7ee2ed
Revises: 10f90bcb56aa
Create Date: 2022-06-27 12:39:38.689265

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '30741c7ee2ed'
down_revision = '10f90bcb56aa'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_unique_constraint('unique_date_time', 'nifty_day_data', ['date', 'time'])
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint('unique_date_time', 'nifty_day_data', type_='unique')
    # ### end Alembic commands ###
