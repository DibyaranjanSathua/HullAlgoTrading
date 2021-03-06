"""add unique constraint

Revision ID: ecb62a320f39
Revises: 41dba14dda03
Create Date: 2022-04-26 21:21:14.612190

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'ecb62a320f39'
down_revision = '41dba14dda03'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_unique_constraint(None, 'historical_price', ['option_strike_id', 'ticker_datetime'])
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint(None, 'historical_price', type_='unique')
    # ### end Alembic commands ###
