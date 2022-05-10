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
