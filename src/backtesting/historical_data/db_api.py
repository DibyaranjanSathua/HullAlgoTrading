"""
File:           db_api.py
Author:         Dibyaranjan Sathua
Created on:     16/04/22, 3:55 pm
"""
from typing import List
import datetime
from typing import Optional
import sqlite3
from pathlib import Path


class DBApi:
    """ Connect to historical database and fetch required data """

    def __init__(self, database_file: Path):
        self._database_file: Path = database_file
        if not self._database_file.is_file():
            raise FileNotFoundError(f"Database file {database_file} doesn't exist")
        self._db_conn: Optional[sqlite3.Connection] = None
        self._db_cursor: Optional[sqlite3.Cursor] = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()

    def connect(self) -> None:
        """ Connect to database """
        self._db_conn = sqlite3.connect(
            self._database_file.as_posix(),
            detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
        )
        self._db_conn.row_factory = sqlite3.Row
        self._db_cursor = self._db_conn.cursor()

    def fetch_historical_data(
            self, index: str, strike: int, option_type: str, expiry: datetime.date
    ) -> List[sqlite3.Row]:
        """ Fetch minute bu minute historical data from database """
        expiry_str: str = expiry.strftime("%Y-%m-%d")
        sql = f"""
        SELECT * FROM HistoricalPrice 
        INNER JOIN Series ON HistoricalPrice.series = Series.id 
        INNER JOIN Scrip ON Series.scrip = Scrip.id 
        WHERE Scrip.name = :index 
        AND Series.expiry = date(:expiry) 
        AND Series.value = :strike 
        AND Series.type = :option_type 
        ORDER BY HistoricalPrice.dt ASC;
        """
        params = {
            "index": index,
            "expiry": expiry_str,
            "strike": strike,
            "option_type": option_type
        }
        self._db_cursor.execute(sql, params)
        return self._db_cursor.fetchall()

    def fetch_series_data(
            self, index: str, strike: int, option_type: str, expiry: datetime.date
    ) -> sqlite3.Row:
        """ Fetch the series for the given strike, option type and expiry """
        expiry_str: str = expiry.strftime("%Y-%m-%d")
        sql = f"""
        SELECT * FROM Series  
        INNER JOIN Scrip ON Series.scrip = Scrip.id 
        WHERE Scrip.name = :index 
        AND Series.expiry = date(:expiry) 
        AND Series.value = :strike 
        AND Series.type = :option_type;
        """
        params = {
            "index": index,
            "expiry": expiry_str,
            "strike": strike,
            "option_type": option_type
        }
        self._db_cursor.execute(sql, params)
        return self._db_cursor.fetchone()

    def is_holiday(self, dt: datetime.date) -> bool:
        """
        This function check if a given date data exist in db. If no data exist, then the
        given date is a holiday.
        """
        start_datetime = datetime.datetime.combine(dt, datetime.time(hour=9, minute=15))
        end_datetime = datetime.datetime.combine(dt, datetime.time(hour=15, minute=30))
        start_str: str = start_datetime.strftime("%Y-%m-%d %H:%M:%S")
        end_str: str = end_datetime.strftime("%Y-%m-%d %H:%M:%S")
        sql = f"""
        SELECT * FROM HistoricalPrice
        WHERE HistoricalPrice.dt > :start 
        AND HistoricalPrice.dt < :end;
        """
        params = {"start": start_str, "end": end_str}
        self._db_cursor.execute(sql, params)
        return self._db_cursor.fetchone() is None

    def disconnect(self):
        """ Disconnect from db """
        self._db_cursor.close()
        self._db_conn.close()


if __name__ == "__main__":
    db_file = "/Users/dibyaranjan/Upwork/client_arun_algotrading/HullAlgoTrading/data/" \
              "database.sqlite"
    with DBApi(Path(db_file)) as db_api:
        series = db_api.fetch_series_data(
            index="NIFTY",
            strike=17400,
            option_type="CE",
            expiry=datetime.date(day=3, month=2, year=2022)
        )

    with DBApi(Path(db_file)) as db_api:
        res = db_api.fetch_historical_data(
            index="NIFTY",
            strike=17400,
            option_type="CE",
            expiry=datetime.date(day=3, month=2, year=2022)
        )

    with DBApi(Path(db_file)) as db_api:
        holiday = db_api.is_holiday(dt=datetime.date(day=3, month=2, year=2022))
        print(holiday)
