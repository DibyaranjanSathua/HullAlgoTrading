"""
File:           instrument.py
Author:         Dibyaranjan Sathua
Created on:     16/04/22, 1:08 pm
"""
from typing import Optional
from dataclasses import dataclass
import datetime
import enum


class InstrumentAction(enum.IntEnum):
    BUY = 1
    SELL = 2


@dataclass()
class Instrument:
    symbol: str
    lot_size: Optional[int]
    expiry: Optional[datetime.date]
    entry: Optional[datetime.datetime]
    option_type: Optional[str]
    strike: Optional[int]
    price: float
    exit_price: Optional[float] = 0
    sl_price: Optional[float] = None
    action: Optional[InstrumentAction] = None       # Indicates BUY or SELL


@dataclass()
class CalendarInstrument:
    current_week_instrument: Instrument
    next_week_instrument: Instrument
