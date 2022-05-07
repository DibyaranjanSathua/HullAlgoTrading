"""
File:           models.py
Author:         Dibyaranjan Sathua
Created on:     07/05/22, 7:10 pm
"""
from enum import Enum

from pydantic import BaseModel, Field


class OptionType(str, Enum):
    CE = "CE"
    PE = "PE"


class ActionType(str, Enum):
    Entry = "ET"
    StopLoss = "SL"
    StopLoss1 = "SL1"
    TargetProfit1 = "TP1"
    TargetProfit2 = "TP2"
    TargetProfit3 = "TP3"
    TargetProfit4 = "TP4"


class TradingViewSignal(BaseModel):
    """ Use for tradingview webhook signal """
    user: str
    password: str
    timestamp: str
    exchange_ticker: str = Field(..., alias="exchange:ticker")
    trigger_price: float
    option_type: OptionType
    action: ActionType
    contract_size: int
    market_position_size: int
    prev_market_position_size: int

