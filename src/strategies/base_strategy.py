"""
File:           base_strategy.py
Author:         Dibyaranjan Sathua
Created on:     15/04/22, 6:51 pm
"""
from typing import Dict, Any
from abc import ABC, abstractmethod
import datetime

from src.strategies.enums import ActionType


class BaseStrategy(ABC):
    """ Abstract class contains common functions that needs to be implemented in the child class """
    STRATEGY_CODE: str = ""

    def __init__(self):
        pass

    @abstractmethod
    def entry(self) -> None:
        pass

    @abstractmethod
    def exit(self) -> None:
        pass

    def process_live_tick(self) -> None:
        pass

    def execute(self) -> None:
        pass

    @staticmethod
    def is_market_hour(dt: datetime.datetime) -> bool:
        """ Return True if dt is in market hour 9:15:01 to 3:29:59. dt is IST timezone """
        start_time = datetime.time(hour=9, minute=15)
        end_time = datetime.time(hour=15, minute=30)
        return start_time < dt.time() < end_time

    @staticmethod
    def is_entry_signal(signal: Dict[str, Any]):
        return signal["action"] == ActionType.Entry.value

    @staticmethod
    def trading_session_ends(now: datetime.datetime):
        """ Return true if the time is greater than 3:36 PM else false """
        return now.time().hour == 15 and now.time().minute > 35
