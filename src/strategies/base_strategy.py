"""
File:           base_strategy.py
Author:         Dibyaranjan Sathua
Created on:     15/04/22, 6:51 pm
"""
from typing import Optional
from abc import ABC, abstractmethod

from src.utils.instrument import Instrument


class BaseStrategy(ABC):
    """ Abstract class contains common functions that needs to be implemented in the child class """
    STRATEGY_CODE: str = ""

    def __init__(self):
        pass

    @abstractmethod
    def entry(self, instrument: Instrument) -> None:
        pass

    @abstractmethod
    def exit(self, lot_size: Optional[int] = None) -> None:
        pass

    @abstractmethod
    def process_live_tick(self) -> None:
        pass
