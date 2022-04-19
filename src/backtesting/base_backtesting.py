"""
File:           base_backtesting.py
Author:         Dibyaranjan Sathua
Created on:     16/04/22, 12:17 pm
"""
from typing import Optional
import pandas as pd
from pathlib import Path
import datetime

from src.backtesting.instrument import Instrument
from src.backtesting.config_reader import ConfigReader
from src.backtesting.historical_data.db_api import DBApi
from src.backtesting.exception import ConfigFileError
from src.utils.logger import LogFacade


class BaseBackTesting:
    """ Base class to hold common backtesting methods """
    # Stores the entry trade
    CE_ENTRY_INSTRUMENT: Optional[Instrument] = None
    PE_ENTRY_INSTRUMENT: Optional[Instrument] = None

    def __init__(self, config_file_path: str):
        self._config_file_path: Path = Path(config_file_path)
        self.config: Optional[ConfigReader] = None
        self._logger: LogFacade = LogFacade("base_backtesting")

    def is_entry_taken(self) -> bool:
        """ Returns True if an entry has taken else returns False """
        return self.CE_ENTRY_INSTRUMENT is not None or self.PE_ENTRY_INSTRUMENT is not None

    def execute(self):
        """ execute back-testing """
        self.config = ConfigReader(self._config_file_path)
        if "input_excel_file_path" not in self.config:
            raise ConfigFileError("Missing input_excel_file_path attribute in config file")
        if "output_excel_file_path" not in self.config:
            raise ConfigFileError("Missing output_excel_file_path attribute in config file")
        if "db_file_path" not in self.config:
            raise ConfigFileError("Missing db_file_path attribute in config file")
        if self.config.get("ce_premium_check") is not None and \
                "quantity_per_lot" not in self.config:
            raise ConfigFileError(
                "quantity_per_lot attribute is required when ce_premium_check is set"
            )

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

    def get_current_week_expiry(self, signal_date: datetime.date) -> datetime.date:
        """ Return the current week expiry date """
        # Monday is 0 and Sunday is 6. Thursday is 3
        offset = (3 - signal_date.weekday()) % 7
        expiry = signal_date + datetime.timedelta(days=offset)
        # Check if expiry is a holiday
        with DBApi(Path(self.config["db_file_path"])) as db_api:
            holiday = db_api.is_holiday(expiry)
        if holiday:
            self._logger.info(f"Expiry {expiry} is a holiday")
            expiry -= datetime.timedelta(days=1)
        return expiry

    @staticmethod
    def get_nearest_50_strike(index: int) -> int:
        """ Return the nearest 50 strike less than the index value """
        return int((index // 50) * 50)

    def is_holiday(self, dt: datetime.date) -> bool:
        """ Return True is the day is holiday """
        with DBApi(Path(self.config["db_file_path"])) as db_api:
            return db_api.is_holiday(dt=dt)

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
    def get_entry_strike(index: int):
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
