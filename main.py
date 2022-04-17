"""
File:           main.py
Author:         Dibyaranjan Sathua
Created on:     16/04/22, 12:38 pm
"""
from src.backtesting.hull_ma_strategy import HullMABackTesting


def main():
    config_file_path: str = "/Users/dibyaranjan/Upwork/client_arun_algotrading/HullAlgoTrading/" \
                            "data/config.json"
    HullMABackTesting(config_file_path=config_file_path).execute()


if __name__ == "__main__":
    main()
