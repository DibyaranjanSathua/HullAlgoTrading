"""
File:           instrument.py
Author:         Dibyaranjan Sathua
Created on:     15/04/22, 7:24 pm
"""
from typing import Optional
from dataclasses import dataclass
import datetime

from src.utils.enums import OptionType


@dataclass()
class Instrument:
    symbol: str
    symbol_code: str
    lot_size: Optional[int]
    expiry: Optional[datetime.date]
    exchange_code: Optional[int]
    exchange: Optional[str]
    code: Optional[int]         # Instrument code
    option_type: Optional[OptionType]
    strike: Optional[int]
    entry: Optional[datetime.datetime]
    price: Optional[float]
