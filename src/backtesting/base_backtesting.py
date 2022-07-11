"""
File:           base_backtesting.py
Author:         Dibyaranjan Sathua
Created on:     16/04/22, 12:17 pm
"""
from typing import Optional, List
from pathlib import Path
import datetime
import calendar
import math

import pandas as pd
from sqlalchemy.orm import Session

from src.backtesting.instrument import Instrument, CalendarInstrument
from src.backtesting.config_reader import ConfigReader
from src.backtesting.historical_data.db_api import DBApiPostgres
from src.backtesting.historical_data.database import SessionLocal
from src.backtesting.exception import ConfigFileError
from src.backtesting.historical_data.models import Holiday
from src.backtesting.historical_data.historical_data import HistoricalData
from src.utils.logger import LogFacade


class PairInstrument:
    """ Contains different CE and PE instrument """

    def __init__(self):
        self.CE_ENTRY_INSTRUMENT: Optional[Instrument] = None
        self.PE_ENTRY_INSTRUMENT: Optional[Instrument] = None

    def is_entry_taken(self) -> bool:
        """ Returns True if an entry has taken else returns False """
        return self.CE_ENTRY_INSTRUMENT is not None or self.PE_ENTRY_INSTRUMENT is not None


class Calendar:
    """ Contains CE and PE calendar instrument """

    def __init__(self):
        self.PE_CALENDAR: Optional[CalendarInstrument] = None
        self.CE_CALENDAR: Optional[CalendarInstrument] = None

    def is_entry_taken(self) -> bool:
        """ Returns True if an entry has taken else returns False """
        return self.PE_CALENDAR is not None or self.CE_CALENDAR is not None


class BaseBackTesting:
    """ Base class to hold common backtesting methods """

    def __init__(
            self,
            config_file_path: str,
            input_excel_file_path: Optional[str] = None,
            output_excel_file_path: Optional[str] = None
    ):
        self._config_file_path: Path = Path(config_file_path)
        self._input_excel_file_path = input_excel_file_path
        self._output_excel_file_path = output_excel_file_path
        self._logger: LogFacade = LogFacade("base_backtesting")
        self._holidays: Optional[List[Holiday]] = None
        self.config: Optional[ConfigReader] = None
        self.session: Optional[Session] = None
        self.historical_data: Optional[HistoricalData] = None

    def __del__(self):
        if self.session is not None:
            self.session.close()

    def execute(self):
        """ execute back-testing """
        self.session = SessionLocal()
        self.historical_data = HistoricalData(session=self.session)
        self.config = ConfigReader(self._config_file_path)
        if "input_excel_file_path" not in self.config:
            raise ConfigFileError("Missing input_excel_file_path attribute in config file")
        if "output_excel_file_path" not in self.config:
            raise ConfigFileError("Missing output_excel_file_path attribute in config file")
        # if "db_file_path" not in self.config:
        #     raise ConfigFileError("Missing db_file_path attribute in config file")
        if self.config.get("ce_premium_check") is not None and \
                "quantity_per_lot" not in self.config:
            raise ConfigFileError(
                "quantity_per_lot attribute is required when ce_premium_check is set"
            )
        if self._input_excel_file_path is not None:
            self.config["input_excel_file_path"] = self._input_excel_file_path
        if self._output_excel_file_path is not None:
            self.config["output_excel_file_path"] = self._output_excel_file_path

    @staticmethod
    def read_input_excel_to_df(filepath: Path) -> pd.DataFrame:
        """ Read the input xls and store the contents in a dataframe """
        if not filepath.is_file():
            raise FileNotFoundError(f"{filepath} doesn't exist")
        return pd.read_excel(
            filepath,
            sheet_name=0,
            header=0,
            usecols="A:F",
            converters={"Date/Time": pd.to_datetime}
        )

    @staticmethod
    def save_df_to_excel(df: pd.DataFrame, filepath: Path) -> None:
        """ Save the dataframe to excel """
        df.to_excel(filepath, header=True, index=False)

    @staticmethod
    def get_current_week_expiry(signal_date: datetime.date) -> datetime.date:
        """ Return the current week expiry date """
        # Monday is 0 and Sunday is 6. Thursday is 3
        offset = (3 - signal_date.weekday()) % 7
        expiry = signal_date + datetime.timedelta(days=offset)
        return expiry

    @staticmethod
    def get_next_week_expiry(signal_date: datetime.date) -> datetime.date:
        """ Return the next week expiry date """
        # Monday is 0 and Sunday is 6. Thursday is 3
        # Calculate current week expiry and add 7 days
        offset = (3 - signal_date.weekday()) % 7
        expiry = signal_date + datetime.timedelta(days=offset) + datetime.timedelta(days=7)
        return expiry

    @staticmethod
    def get_current_month_expiry(signal_date: datetime.date) -> datetime.date:
        """ Return current month last thursday date """
        year = signal_date.year
        month = signal_date.month
        month_calendar = calendar.monthcalendar(year=year, month=month)
        thursday = max(month_calendar[-1][calendar.THURSDAY], month_calendar[-2][calendar.THURSDAY])
        expiry = datetime.date(year=year, month=month, day=thursday)
        if signal_date > expiry:
            # Get the next month expiry
            month += 1
            if month > 12:
                month = 1
                year += 1
            month_calendar = calendar.monthcalendar(year=year, month=month)
            thursday = max(month_calendar[-1][calendar.THURSDAY],
                           month_calendar[-2][calendar.THURSDAY])
            expiry = datetime.date(year=year, month=month, day=thursday)
        return expiry

    def get_expiry(self, signal_date: datetime.date) -> datetime.date:
        """ Return either weekly or monthly expiry depends on the setting in config file """
        monthly_expiry = self.config.get("monthly_expiry", False)
        expiry = self.get_current_month_expiry(signal_date) if monthly_expiry \
            else self.get_current_week_expiry(signal_date)
        return self.get_valid_expiry(expiry=expiry)

    def get_valid_expiry(self, expiry: datetime.date) -> datetime.date:
        """ Return a valid expiry which is not a weekend nor a holiday """
        holiday = self.is_holiday(expiry)
        if holiday:
            valid_expiry = expiry
            while True:
                valid_expiry -= datetime.timedelta(days=1)
                # Saturday or Sunday or holiday
                if valid_expiry.weekday() in (5, 6) or \
                        self.is_holiday(valid_expiry):
                    continue
                break
            return valid_expiry
        return expiry

    def is_valid_trading_day(self, date: datetime.date) -> bool:
        """ Check if the given date is between Monday to Friday and is not a holiday """
        if date.weekday() in (5, 6) or self.is_holiday(date):
            return False
        return True

    @staticmethod
    def get_nearest_50_strike(index: float) -> int:
        """ Return the nearest 50 strike less than the index value """
        return int((index // 50) * 50)

    @staticmethod
    def get_nearest_100_strike(index: float, n: int = 0) -> int:
        """ Add n to the index and return nearest 100 """
        index += n
        # For positive n, round down to nearest 100. For negative n, round up to nearest 100
        # func = math.floor if n >= 0 else math.ceil
        return round(index / 100) * 100

    def is_holiday(self,  dt: datetime.date) -> bool:
        """ Return True is the day is holiday """
        if self._holidays is None:
            self._holidays = DBApiPostgres.get_holidays(self.session)
        return next((x for x in self._holidays if x.holiday_date == dt), None) is not None
        # return DBApiPostgres.is_holiday(self.session, dt=dt)

    def get_next_valid_date(self, current_exit_date: datetime.date) -> datetime.date:
        """ Return the next valid date which is not a weekend nor a holiday """
        # If the exit date is Friday, then add 3 days
        # Monday is 0 and Sunday is 6
        self._logger.info(f"Finding the next valid exit date for {current_exit_date}")
        next_exit_date = current_exit_date
        while True:
            next_exit_date += datetime.timedelta(days=1)
            # Saturday or Sunday or holiday
            if next_exit_date.weekday() in (5, 6) or self.is_holiday(dt=next_exit_date):
                continue
            break
        return next_exit_date

    def get_market_hour_datetime(self, signal_datetime: datetime.datetime) -> datetime.datetime:
        """
        Return the actual datetime of signal.
        If time < 9:15, return same day 9:15
        If time > 3:29, return next day 9:15
        """
        signal_date = signal_datetime.date()
        signal_time = signal_datetime.time()
        if signal_time.hour == 9 and signal_time.minute < 15:
            return datetime.datetime.combine(signal_date, datetime.time(hour=9, minute=15))
        if signal_time.hour == 15 and signal_time.minute > 29:
            entry_date = self.get_next_valid_date(signal_date)
            return datetime.datetime.combine(entry_date, datetime.time(hour=9, minute=15))
        return signal_datetime

    @staticmethod
    def get_entry_strike(index: float):
        """ Return the entry index """
        return BaseBackTesting.get_nearest_50_strike(index)

    def is_data_missing(self, strike: int, dt: datetime.datetime):
        """ Check if the data for the specific strike is missing.
        This needs to be configure in the config file manually
        """
        if "missing_data" in self.config:
            data_exist = next(
                (
                    x
                    for x in self.config["missing_data"]
                    if x["strike"] == strike and x["start_datetime"] <= dt <= x["end_datetime"]
                ),
                None
            )
            return data_exist is not None
        return False

    def is_nifty_data_missing(self, dt: datetime.datetime):
        """
        Check if the minute data for nifty is missing.
        This needs to be configure in the config file manually.
        Strike should be 1 for indicating missing nifty data.
        """
        if "missing_data" in self.config:
            data_exist = next(
                (
                    x
                    for x in self.config["missing_data"]
                    if x["strike"] == 1 and x["start_datetime"] <= dt <= x["end_datetime"]
                ),
                None
            )
            return data_exist is not None
        return False

    def get_nifty_day_open(self, date: datetime.date) -> Optional[float]:
        """ Get the open value for nifty for specific date """
        nifty_data = DBApiPostgres.get_nifty_day_ohlc(self.session, date)
        if nifty_data is None:
            self._logger.warning(f"No nifty data found for {date}")
            return None
        return nifty_data.open
