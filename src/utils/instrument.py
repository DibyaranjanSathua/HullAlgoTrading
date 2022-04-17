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
    lot_size: Optional[int]
    expiry: Optional[datetime.date]
    exchange_code: Optional[int]
    exchange: Optional[str]
    code: Optional[int]         # Instrument code
    option_type: Optional[OptionType]
    strike: Optional[int]
    index: bool

    @classmethod
    def create(cls, data):
        """ Create Instrument class object from the input data """
        lot_size = None
        expiry = None
        option_type = None
        strike = None
        if "lotSize" in data:
            lot_size = int(data["lotSize"])
        if "expiry" in data:
            expiry = datetime.datetime.fromtimestamp(data["expiry"]).date()
        code = int(data["code"])
        index = data.get("index", False)
        if index:
            # Check if the data is for Option or Fut Instrument
            symbol_parts = data["symbol"].split(" ")
            if symbol_parts[-1] == "CE":
                option_type = OptionType.CE
                strike = int(symbol_parts[-2])
            elif symbol_parts[-1] == "PE":
                option_type = OptionType.PE
                strike = int(symbol_parts[-2])
            elif symbol_parts[-1] == "FUT":
                option_type = OptionType.FUT
                strike = None

        return cls(
            symbol=data["symbol"],
            lot_size=lot_size,
            expiry=expiry,
            exchange_code=data["exchange_code"],
            exchange=data["exchange"],
            code=code,
            option_type=option_type,
            strike=strike,
            index=index
        )
