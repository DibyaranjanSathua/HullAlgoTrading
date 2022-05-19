"""
File:           models.py
Author:         Dibyaranjan Sathua
Created on:     25/04/22, 6:50 pm
"""
from sqlalchemy import String, BigInteger, ForeignKey, Column, Date, DateTime, Integer, Float, \
    UniqueConstraint
from sqlalchemy.orm import relationship

from src.backtesting.historical_data.database import Base


class StockIndex(Base):
    """ Stores stock index name """
    __tablename__ = "stock_index"

    id = Column(BigInteger, primary_key=True)
    name = Column(String(50), unique=True, index=True)

    option_strikes = relationship("OptionStrike", back_populates="stock_index")


class OptionStrike(Base):
    """ Stores option strikes for index """
    __tablename__ = "option_strike"

    id = Column(BigInteger, primary_key=True)
    name = Column(String(50), unique=True)
    stock_index_id = Column(BigInteger, ForeignKey("stock_index.id"))
    expiry = Column(Date, index=True)
    strike = Column(Integer, index=True)
    option_type = Column(String(5), index=True)

    stock_index = relationship("StockIndex", back_populates="option_strikes")
    historical_prices = relationship("HistoricalPrice", back_populates="option_strike")

    def __repr__(self):
        return f"[{self.id}] {self.name} {self.expiry}"


class HistoricalPrice(Base):
    """ Store historical price of """
    __tablename__ = "historical_price"

    id = Column(BigInteger, primary_key=True)
    open = Column(Float(precision=2), nullable=True)
    high = Column(Float(precision=2), nullable=True)
    low = Column(Float(precision=2), nullable=True)
    close = Column(Float(precision=2), nullable=True)
    volume = Column(Integer, nullable=True)
    oi = Column(Integer, nullable=True)
    ticker_datetime = Column(DateTime, index=True)
    option_strike_id = Column(BigInteger, ForeignKey("option_strike.id"))

    option_strike = relationship("OptionStrike", back_populates="historical_prices")

    __table_args__ = (
        UniqueConstraint("option_strike_id", "ticker_datetime", name="unique_strike_dt"),
    )

    def __repr__(self):
        return f"[{self.id}] {self.option_strike.name} {self.option_strike.expiry} " \
               f"{self.ticker_datetime} {self.close}"


class Holiday(Base):
    """ List of holidays """
    __tablename__ = "holiday"

    id = Column(BigInteger, primary_key=True)
    holiday_date = Column(Date, index=True, unique=True)


class NiftyDayData(Base):
    __tablename__ = "nifty_day_data"

    id = Column(BigInteger, primary_key=True)
    date = Column(Date, index=True, unique=True)
    open = Column(Float(precision=2), nullable=True)
    high = Column(Float(precision=2), nullable=True)
    low = Column(Float(precision=2), nullable=True)
    close = Column(Float(precision=2), nullable=True)

    def __repr__(self):
        return f"[{self.id}] {self.date} {self.open} {self.high} {self.low} {self.close}"
