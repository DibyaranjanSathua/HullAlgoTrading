# HullAlgoTrading
Hull moving average algo trading

## BackTesting guidelines
1. Input excel should contain only one sheet. If it contains multiple sheets, ensure that the first sheet contains the inputs for the scripts.
2. First row should be the header

## Alembic command
1. alembic revision --autogenerate -m "nifty day data table"
2. alembic upgrade head
