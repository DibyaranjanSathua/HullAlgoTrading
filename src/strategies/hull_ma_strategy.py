"""
File:           hull_ma_strategy.py
Author:         Dibyaranjan Sathua
Created on:     10/05/22, 6:08 am
"""
from typing import Optional, Dict, Any
import datetime
from collections import deque
import json
from pathlib import Path
import pickle

from src.strategies.base_strategy import BaseStrategy
from src.fyers.fyers import FyersApi
from src.utils.redis_backend import RedisBackend
from src.utils.instrument import Instrument
from src.utils import utc2ist, istnow
from src.utils.logger import LogFacade
from src import DATA_DIR


logger: LogFacade = LogFacade.get_logger("hull_ma_trading_strategy")


class HullMATradingStrategy(BaseStrategy):
    """ Hull Moving avergae strategy """
    STRATEGY_CODE: str = "strategy1"

    def __init__(self):
        super(HullMATradingStrategy, self).__init__()
        self._redis: RedisBackend = RedisBackend()
        self._fyers_api: FyersApi = FyersApi()
        self._entry_instrument: Optional[Instrument] = None
        self._signal: Optional[Dict[str, Any]] = None

    def setup(self) -> None:
        self._redis.connect()
        self._redis.subscribe(HullMATradingStrategy.STRATEGY_CODE)
        self._fyers_api.setup()
        # If entry instrument pickle file exist, load the instrument from the file
        self._read_entry_instrument()

    def entry(self) -> None:
        """ Entry logic """
        if self._entry_instrument is not None:
            print(f"An entry has already been taken. Ignoring this signal")
            return None
        self._get_entry_instrument()
        logger.info(
            f"Entry taken for symbol {self._entry_instrument.symbol} at "
            f"{self._entry_instrument.entry} at price {self._entry_instrument.price}"
        )

    def exit(self) -> None:
        """ Exit logic """
        if self._entry_instrument is None:
            print(f"No open position. Ignoring this signal")
            return None
        market_quotes = self._fyers_api.get_market_quotes(self._entry_instrument.symbol_code)
        logger.info(
            f"Exit taken for symbol {self._entry_instrument.symbol} at "
            f"{self._signal['timestamp']} at price {market_quotes['prev_close_price']}"
        )

    def execute(self) -> None:
        """ main function which will run forever and execute the strategy in an infinite loop """
        print(f"Setting up the strategy")
        self.setup()
        print(f"Starting execution of strategy {HullMATradingStrategy.STRATEGY_CODE}")
        super(HullMATradingStrategy, self).execute()
        import time
        while True:
            self._signal = self.get_tradingview_signal()
            now = istnow()
            if self._signal is None:
                # SL logic
                pass
            else:
                if self.market_hour(now):
                    if self.is_entry_signal(self._signal):
                        self.entry()
                    else:
                        self.exit()
                else:
                    # After market hour logic
                    pass

                    #     if self._entry_instrument is None:
                    #         print(f"Taken entry")
                    #         self.get_entry_instrument(signal)
                    #     else:
                    #         print(f"Entry already taken")
                    #
                    # print(self._entry_instrument)
            time.sleep(5)

    def get_tradingview_signal(self) -> Optional[Dict[str, Any]]:
        """ Check for any trading view signal """
        # get_message should only be called after subscribing to a channel
        message: str = self._redis.get_message()
        signal: Optional[Dict[str, Any]] = None
        if message:
            print(f"Signal received from TradingView: {message}")
            signal = json.loads(message)
            # Signal datetime from tradingview will be in UTC
            signal["timestamp"] = datetime.datetime.fromisoformat(
                signal["timestamp"].strip("Z")
            )
            signal["timestamp"] = utc2ist(signal["timestamp"])
        return signal

    def _get_entry_instrument(self):
        """ Get the CE entry instrument """
        expiry = self.get_current_week_expiry(self._signal["timestamp"].date())
        exchange, ticker = self._signal["exchange_ticker"].split(":")
        strike_price = self.get_nearest_50_strike(self._signal["trigger_price"])
        symbol_details = self._fyers_api.fyers_symbol_parser.get_fyers_symbol_name(
            ticker=ticker,
            strike_price=strike_price,
            expiry=expiry,
            option_type=self._signal["option_type"]
        )
        market_quotes = self._fyers_api.get_market_quotes(symbol_details["symbol_code"])
        self._entry_instrument = Instrument(
            symbol=symbol_details["symbol"],
            symbol_code=symbol_details["symbol_code"],
            lot_size=self._signal["contract_size"],
            expiry=expiry,
            exchange_code=0,
            exchange=exchange,
            code=symbol_details["code"],
            option_type=self._signal["option_type"],
            strike=strike_price,
            entry=self._signal["timestamp"],
            price=market_quotes["prev_close_price"]
        )
        self._write_entry_instrument()

    def get_current_week_expiry(self, dt: datetime.date):
        """ Return the current week expiry using Fyers API (using symbol CSV file) """
        return self._fyers_api.fyers_symbol_parser.get_current_week_expiry(signal_date=dt)

    def _write_entry_instrument(self):
        """ Save entry instrument to a file """
        with open(self.entry_instrument_file, mode="wb") as fp_:
            pickle.dump(self._entry_instrument, fp_)

    def _read_entry_instrument(self):
        """ Read the entry instrument """
        if self.entry_instrument_file.is_file():
            with open(self.entry_instrument_file, mode="rb") as fp_:
                self._entry_instrument = pickle.load(fp_)

    @staticmethod
    def get_nearest_50_strike(index: float) -> int:
        """ Return the nearest 50 strike less than the index value """
        return int((index // 50) * 50)

    @property
    def entry_instrument_file(self) -> Path:
        return DATA_DIR / "entry_instrument.pkl"


if __name__ == "__main__":
    strategy = HullMATradingStrategy()
    strategy.execute()
