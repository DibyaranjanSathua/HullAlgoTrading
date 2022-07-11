"""
File:           historical_data.py
Author:         Dibyaranjan Sathua
Created on:     03/07/22, 1:17 pm
"""
from typing import Optional, List
import datetime

from sqlalchemy.orm import Session

from src.backtesting.historical_data.models import HistoricalPrice
from src.backtesting.historical_data.db_api import DBApiPostgres
from src.utils.logger import LogFacade


class HistoricalData:
    """
    Fetch option histrocial data for couple of days and cache it so that db interaction reduces,
    """
    def __init__(self, session: Session):
        self._session: Optional[Session] = session
        self._start_date: Optional[datetime.date] = None
        self._end_date: Optional[datetime.date] = None
        self._ce_historical_data: List[HistoricalPrice] = []
        self._pe_historical_data: List[HistoricalPrice] = []
        self._logger: LogFacade = LogFacade("historical_data")

    def fetch_from_db(self, index: str):
        """ Fetch data from db and store it to a variable """
        self._logger.info(
            f"Fetching CE option data for expiry in range {self._start_date} and {self._end_date}"
        )
        self._ce_historical_data = DBApiPostgres.fetch_historical_data_by_date(
            session=self._session,
            index=index,
            option_type="CE",
            start_date=self._start_date,
            end_date=self._end_date
        )
        self._logger.info(
            f"Fetching PE option data for expiry in range {self._start_date} and {self._end_date}"
        )
        self._pe_historical_data = DBApiPostgres.fetch_historical_data_by_date(
            session=self._session,
            index=index,
            option_type="PE",
            start_date=self._start_date,
            end_date=self._end_date
        )

    def get_data(
            self,
            index: str,
            strike: int,
            option_type: str,
            expiry: datetime.date
    ) -> List[HistoricalPrice]:
        """ Return historical data for specific index, strike, option_type and expiry """
        self._logger.info(
            f"Getting historical data for {index} {strike} {option_type} for expiry {expiry}"
        )
        # If start date or end date is None, fetch data from db
        if self._start_date is None or self._end_date is None:
            # Fetch data for the expiry month
            self._start_date = expiry.replace(day=1)
            self._end_date = self._start_date + datetime.timedelta(days=30)
            self.fetch_from_db(index)
        # If expiry date is greater than self._end_date, fetch data from db with
        # new start date and end date
        if expiry > self._end_date:
            self._start_date = self._end_date + datetime.timedelta(days=1)
            self._end_date = self._start_date + datetime.timedelta(days=30)
            self.fetch_from_db(index)

        if option_type == "CE":
            historical_data = self._ce_historical_data
        else:
            historical_data = self._pe_historical_data
        return [
            x
            for x in historical_data
            if x.option_strike.strike == strike and x.option_strike.expiry == expiry
        ]

