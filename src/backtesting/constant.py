"""
File:           constant.py
Author:         Dibyaranjan Sathua
Created on:     16/04/22, 1:19 pm
"""


class SignalType:
    ENTRY: str = "ET"
    EXIT: str = "EX"
    ENTRY_LONG: str = "ETL"     # For calendar spread. PE calendar
    EXIT_LONG: str = "EXL"      # For calendar spread. PE calendar
    ENTRY_SHORT: str = "ETS"    # For calendar spread. CE calendar
    EXIT_SHORT: str = "EXS"    # For calendar spread. CE calendar


class ExitType:
    SL_EXIT: str = "SL Exit"
    EXIT_SIGNAL: str = "Exit Signal"
    EXPIRY_EXIT: str = "Expiry Exit"
    MISSING_DATA: str = "Missing Data"
    CE_PREMIUM_EXIT: str = "CE not traded due to premium check"
    NO_TRADE: str = "No Trade"
    SOFT_EXIT: str = "Soft Exit"
    INVALID_EXIT: str = "Invalid Exit"
    TAKE_PROFIT_EXIT: str = "Take Profit Exit"


class EntryType:
    ENTRY_SIGNAL: str = "Entry Signal"
    SOFT_ENTRY: str = "Soft Entry"
