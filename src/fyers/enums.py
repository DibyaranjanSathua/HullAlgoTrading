"""
File:           enums.py
Author:         Dibyaranjan Sathua
Created on:     09/05/22, 1:22 pm
"""
from enum import Enum, unique


@unique
class OptionType(Enum):
    CE = "CE"
    PE = "PE"


@unique
class OrderAction(Enum):
    BUY = 1
    SELL = -1


@unique
class OrderType(Enum):
    LIMIT_ORDER = 1
    MARKET_ORDER = 2
    STOP_ORDER = 3
    STOP_LIMIT_ORDER = 4


@unique
class ProductType(Enum):
    INTRADAY = "Intraday"
    CNC = "CNC"
    MARGIN = "Margin"
    COVER_ORDER = "Cover Order"
    BRACKET_ORDER = "Bracket Order"


@unique
class OrderValidity(Enum):
    DAY = "DAY"
    IOC = "IOC"
