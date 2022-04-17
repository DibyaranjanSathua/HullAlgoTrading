"""
File:           instrument.py
Author:         Dibyaranjan Sathua
Created on:     16/04/22, 1:08 pm
"""
from typing import Optional
from dataclasses import dataclass
import datetime


@dataclass()
class Instrument:
    symbol: str
    lot_size: Optional[int]
    expiry: Optional[datetime.date]
    entry: Optional[datetime.datetime]
    option_type: Optional[str]
    strike: Optional[int]
    price: float
