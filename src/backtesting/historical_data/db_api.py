"""
File:           db_api.py
Author:         Dibyaranjan Sathua
Created on:     16/04/22, 3:55 pm
"""
from typing import List, Dict, Any
import datetime
from typing import Optional
import sqlite3
from pathlib import Path

from sqlalchemy.orm import Session
from sqlalchemy.dialects import postgresql

from src.backtesting.historical_data.models import Holiday, StockIndex, OptionStrike, \
    HistoricalPrice


class DBApiPostgres:
    """ Connect to postgres database and perform CRUD operations """

    @staticmethod
    def create_holiday(session: Session, dt: datetime.date) -> Holiday:
        """ Add holiday to the holiday table """
        holiday = Holiday(holiday_date=dt)
        session.add(holiday)
        session.commit()
        session.refresh(holiday)
        return holiday

    @staticmethod
    def is_holiday(session: Session, dt: datetime.date):
        """ Check if the provided date is a holiday """
        return session.query(Holiday).filter(Holiday.holiday_date == dt).first() is not None

    @staticmethod
    def create_stock_index(session: Session, index: str) -> StockIndex:
        """ Check if the index exist in database. If not then add it to db """
        stock_index, created = DBApiPostgres.get_or_create(session, StockIndex, name=index)
        if created:
            print(f"Added stock index {index}")
        else:
            print(f"Exist stock index {index}")
        return stock_index

    @staticmethod
    def create_option_strike(
            session: Session,
            name: str,
            stock_index_id: int,
            expiry: datetime.date,
            strike: int,
            option_type: str
    ) -> OptionStrike:
        """ Check and create option strike in database """
        option_strike, created = DBApiPostgres.get_or_create(
            session,
            OptionStrike,
            name=name,
            stock_index_id=stock_index_id,
            expiry=expiry,
            strike=strike,
            option_type=option_type
        )
        if created:
            print(f"Added [{option_strike.id}] {name} {strike} {option_type} {expiry}")
        else:
            print(f"Exist [{option_strike.id}] {name} {strike} {option_type} {expiry}")
        return option_strike

    @staticmethod
    def create_historical_price(
            session: Session,
            open: float,
            high: float,
            low: float,
            close: float,
            volume: int,
            oi: int,
            ticker_datetime: datetime.datetime,
            option_strike_id: int
    ):
        instance = session.query(HistoricalPrice).filter(
            HistoricalPrice.option_strike_id == option_strike_id,
            HistoricalPrice.ticker_datetime == ticker_datetime
        ).first()
        if instance:
            print(f"Exist historical price for {option_strike_id} {ticker_datetime}")
            return instance
        else:
            print(f"Adding historical price for {option_strike_id} {ticker_datetime}")
            instance = HistoricalPrice(
                open=open,
                high=high,
                low=low,
                close=close,
                volume=volume,
                oi=oi,
                ticker_datetime=ticker_datetime,
                option_strike_id=option_strike_id
            )
            session.add(instance)
            session.commit()
            session.refresh(instance)
            return instance

    @staticmethod
    def create_bulk_historical_price(session: Session, items: List[Dict[str, Any]]):
        """ Create bulk insert. For row exist, it will do nothing  """
        session.execute(
            postgresql.insert(HistoricalPrice.__table__).values(items).on_conflict_do_nothing(
                constraint="unique_strike_dt"
            )
        )

    @staticmethod
    def fetch_historical_data(
            session: Session, index: str, strike: int, option_type: str, expiry: datetime.date
    ) -> List[HistoricalPrice]:
        result = session.query(HistoricalPrice).join(OptionStrike).join(StockIndex).filter(
            StockIndex.name == index,
            OptionStrike.strike == strike,
            OptionStrike.option_type == option_type,
            OptionStrike.expiry == expiry
        ).order_by(HistoricalPrice.ticker_datetime)
        # Due to lazy loading, database base operation will not be performed until we request for it
        return list(result)

    @staticmethod
    def fetch_option_strike(
        session: Session, index: str, strike: int, option_type: str, expiry: datetime.date
    ) -> OptionStrike:
        return session.query(OptionStrike).join(StockIndex).filter(
            StockIndex.name == index,
            OptionStrike.strike == strike,
            OptionStrike.option_type == option_type,
            OptionStrike.expiry == expiry
        ).first()

    @staticmethod
    def get_or_create(session: Session, model, **kwargs):
        instance = session.query(model).filter_by(**kwargs).first()
        if instance:
            return instance, False
        else:
            instance = model(**kwargs)
            session.add(instance)
            session.commit()
            session.refresh(instance)
            return instance, True


class DBApiSqLite:
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
    from src.backtesting.historical_data.database import SessionLocal
    with SessionLocal() as session:
        data = DBApiPostgres.fetch_historical_data(
            session=session,
            index="NIFTY",
            strike=9250,
            option_type="CE",
            expiry=datetime.date(day=27, month=10, year=2016)
        )
        print(type(data))       # <class 'sqlalchemy.orm.query.Query'>
        print(type(data[0]))    # <class 'src.backtesting.historical_data.models.HistoricalPrice'>
        for x in data:
            print(x)

    # db_file = "/Users/dibyaranjan/Upwork/client_arun_algotrading/HullAlgoTrading/data/" \
    #           "database.sqlite"
    # with DBApiSqLite(Path(db_file)) as db_api:
    #     series = db_api.fetch_series_data(
    #         index="NIFTY",
    #         strike=17400,
    #         option_type="CE",
    #         expiry=datetime.date(day=3, month=2, year=2022)
    #     )
    #
    # with DBApiSqLite(Path(db_file)) as db_api:
    #     res = db_api.fetch_historical_data(
    #         index="NIFTY",
    #         strike=17400,
    #         option_type="CE",
    #         expiry=datetime.date(day=3, month=2, year=2022)
    #     )
    #
    # with DBApiSqLite(Path(db_file)) as db_api:
    #     holiday = db_api.is_holiday(dt=datetime.date(day=12, month=1, year=2022))
    #     print(holiday)
