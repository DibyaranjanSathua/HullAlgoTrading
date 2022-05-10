"""
File:           enums.py
Author:         Dibyaranjan Sathua
Created on:     10/05/22, 7:02 am
"""
from enum import Enum


class ActionType(Enum):
    Entry = "ET"
    StopLoss = "SL"
    StopLoss1 = "SL1"
    TargetProfit1 = "TP1"
    TargetProfit2 = "TP2"
    TargetProfit3 = "TP3"
    TargetProfit4 = "TP4"
