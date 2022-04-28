"""
File:           csv2db.py
Author:         Dibyaranjan Sathua
Created on:     25/04/22, 11:32 pm

This script will parse the historical data from csv and save it to postgres db.
"""
from dataclasses import dataclass
import datetime
import calendar
import re
from pathlib import Path
import csv

from sqlalchemy.orm import Session

from src.backtesting.historical_data.db_api import DBApiPostgres
from src.backtesting.historical_data.database import SessionLocal


@dataclass()
class Instrument:
    index: str
    name: str
    strike: int
    expiry: datetime.date
    option_type: str


class CSV2DB:
    """ Class containing function to parse csv file and import historical data to postgres """

    def __init__(self):
        self._filename_regex = re.compile(r"([A-Za-z]+)(\d+)([A-Za-z]+)(\d+)([A-Za-z]+)")
        pass

    def process_top_level_directory(self, directory_path: Path):
        """ Process the top level directory containing each year as subdirectory """
        with SessionLocal() as session:
            for dir_path in directory_path.iterdir():
                if dir_path.is_dir():
                    print(f"Processing {dir_path}")
                    for file_path in dir_path.iterdir():
                        if file_path.is_file() and file_path.suffix == ".csv":
                            self.import_csv(session, file_path, bulk_update=True)

    def process_csv_file(self, csv_filepath: Path):
        """ Process a csv file and import data to database """
        with SessionLocal() as session:
            self.import_csv(session, csv_filepath, bulk_update=False)

    def import_csv(self, session: Session, csv_filepath: Path, bulk_update: bool):
        """ Process a csv file and upload """
        print(f"Processing {csv_filepath}")
        filename = csv_filepath.stem
        instrument = self.parse_filename(session, filename)
        # print(instrument)
        # Create StockIndex model
        stock_index = DBApiPostgres.create_stock_index(
            session, index=instrument.index
        )
        # Create OptionStrike model
        option_strike = DBApiPostgres.create_option_strike(
            session,
            name=instrument.name,
            stock_index_id=stock_index.id,
            expiry=instrument.expiry,
            strike=instrument.strike,
            option_type=instrument.option_type
        )
        # Add the csv file data to db
        self.read_csv(
            session=session,
            csv_filepath=csv_filepath,
            option_strike=option_strike,
            bulk_update=bulk_update
        )

    @staticmethod
    def read_csv(session: Session, csv_filepath: Path, option_strike, bulk_update: bool):
        """ Process csv file and add it to database """
        items = []
        with open(csv_filepath, mode="r", newline="") as csv_file:
            reader = csv.DictReader(csv_file)
            for row in reader:
                # Headers: Ticker,Date,Time,Open,High,Low,Close,Volume,OI
                # print(row["Ticker"], row["Date"], row["Time"], float(row["Close"]))
                ticker_date = datetime.datetime.strptime(row["Date"], "%Y%m%d").date()
                ticker_time = datetime.datetime.strptime(row["Time"], "%H:%M:%S").time()
                ticket_datetime = datetime.datetime.combine(ticker_date, ticker_time)
                row_dict = {
                    "open": float(row["Open"]),
                    "high": float(row["High"]),
                    "low": float(row["Low"]),
                    "close": float(row["Close"]),
                    "volume": int(row["Volume"]),
                    "oi": int(row["OI"]),
                    "ticker_datetime": ticket_datetime,
                    "option_strike_id": option_strike.id
                }

                if bulk_update:
                    items.append(row_dict)
                else:
                    DBApiPostgres.create_historical_price(session, **row_dict)
        if bulk_update:
            DBApiPostgres.create_bulk_historical_price(session=session, items=items)

    def parse_filename(self, session: Session, filename: str) -> Instrument:
        """ Parse filename to get the instrument """
        match_obj = self._filename_regex.search(filename)
        if match_obj is not None:
            index: str = match_obj.group(1)
            strike: int = int(match_obj.group(2))
            month: int = list(calendar.month_abbr).index(match_obj.group(3).title())
            year: int = 2000 + int(match_obj.group(4))
            option_type: str = match_obj.group(5)
            # If the strike is divisible by 50, then it is a strike else the last 2 digit is day
            # For monthly expiry, day is not specified in the filename but for weekly expiry
            # day is specified.
            if strike % 50 == 0:
                # Get monthly expiry
                expiry = self.get_monthly_expiry(session=session,year=year, month=month)
            else:
                strike: int = int(match_obj.group(2)[:-2])
                day: int = int(match_obj.group(2)[-2:])
                expiry = datetime.date(year=year, month=month, day=day)
            return Instrument(
                index=index, name=filename, strike=strike, expiry=expiry, option_type=option_type
            )

    @staticmethod
    def add_holiday_to_db(holiday_filepath: Path):
        """ Read the holiday file and add it to db """
        if not holiday_filepath.is_file():
            raise FileNotFoundError(f"Holiday file {holiday_filepath} doesn't exist")
        with open(holiday_filepath, mode="r") as fh_:
            with SessionLocal() as session:
                for line in fh_:
                    date_str = line.strip()
                    date = datetime.datetime.strptime(date_str, "%d-%b-%Y").date()
                    holiday = DBApiPostgres.is_holiday(session, date)
                    if holiday:
                        print(f"Skipping {date_str}")
                    else:
                        print(f"Adding holiday {date_str} to db")
                        DBApiPostgres.create_holiday(session, date)

    @staticmethod
    def get_monthly_expiry(session: Session, year: int, month: int) -> datetime.date:
        """ Get the date of last thursday of the month """
        month_calendar = calendar.monthcalendar(year=year, month=month)
        thursday = max(month_calendar[-1][calendar.THURSDAY], month_calendar[-2][calendar.THURSDAY])
        expiry = datetime.date(year=year, month=month, day=thursday)
        # Check if expiry is a holiday
        return CSV2DB.get_valid_expiry(session, expiry)

    @staticmethod
    def get_valid_expiry(session: Session, expiry: datetime.date) -> datetime.date:
        """ Return a valid expiry which is not a weekend nor a holiday """
        holiday = DBApiPostgres.is_holiday(session, expiry)
        if holiday:
            valid_expiry = expiry
            while True:
                valid_expiry -= datetime.timedelta(days=1)
                # Saturday or Sunday or holiday
                if valid_expiry.weekday() in (5, 6) or \
                        DBApiPostgres.is_holiday(session, valid_expiry):
                    continue
                break
            return valid_expiry
        return expiry


if __name__ == "__main__":
    # holiday_filepath = Path("/Users/dibyaranjan/Upwork/client_arun_algotrading/HullAlgoTrading"
    #                         "/data/TradingHolidays.csv")
    # CSV2DB().add_holiday_to_db(holiday_filepath)
    top_level_dir = Path("/Users/dibyaranjan/Downloads/NiftyHistoricalData")
    CSV2DB().process_top_level_directory(top_level_dir)
