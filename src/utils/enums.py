"""
File:           enums.py
Author:         Dibyaranjan Sathua
Created on:     16/04/22, 10:42 am
"""
import enum


class OptionType(enum.Enum):
    """ Option Type, Call Option or Put Option """
    CE = 1
    PE = 2
    FUT = 3


class Exchanges(enum.Enum):
    """ Exchanges. Number represents their code """
    NSE = 1
    NFO = 2
