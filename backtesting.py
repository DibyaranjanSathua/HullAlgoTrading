"""
File:           backtesting.py
Author:         Dibyaranjan Sathua
Created on:     28/06/22, 6:04 pm
"""
import argparse
from src.backtesting import HullMABackTesting, RuleEngine2, RuleEngine3
from src.backtesting.exception import BackTestingError
from src.utils.logger import LogFacade


logger = LogFacade.get_logger("main")
ENGINE_MAPPER = {
    "rule_engine1": HullMABackTesting,
    "rule_engine2": RuleEngine2,
    "rule_engine3": RuleEngine3,
}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=str, help="Config file path")
    parser.add_argument("--engine", type=str, help="Rule engine name for backtesting")
    args = parser.parse_args()
    engine = ENGINE_MAPPER.get(args.engine)
    if engine is None:
        raise BackTestingError(f"Invalid engine name. Valid names are {ENGINE_MAPPER.keys()}")
    engine(config_file_path=args.config).execute()


if __name__ == "__main__":
    main()
