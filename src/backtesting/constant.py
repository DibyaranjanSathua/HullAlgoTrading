"""
File:           constant.py
Author:         Dibyaranjan Sathua
Created on:     16/04/22, 1:19 pm
"""


class SignalType:
    ENTRY: str = "ET"
    EXIT: str = "EX"


class ExitType:
    SL_HIT: str = "SL Hit"
    EXIT_SIGNAL: str = "Exit Signal"
    EXPIRY_EXIT: str = "Expiry Exit"
    MISSING_DATA: str = "Missing Data"
