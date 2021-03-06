"""
File:           __init__.py
Author:         Dibyaranjan Sathua
Created on:     15/04/22, 6:50 pm
"""
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)
