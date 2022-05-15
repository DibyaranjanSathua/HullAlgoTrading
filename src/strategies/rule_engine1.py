"""
File:           rule_engine1.py
Author:         Dibyaranjan Sathua
Created on:     10/05/22, 6:08 am
"""
from typing import Optional, Dict, Any
import datetime
import json
from pathlib import Path
import pickle
from collections import deque

from src.strategies.base_strategy import BaseStrategy
from src.fyers.fyers import FyersApi
from src.fyers.enums import OrderAction
from src.utils.redis_backend import RedisBackend
from src.strategies.instrument import Instrument
from src.utils import utc2ist, istnow, make_ist_aware
from src.utils.logger import LogFacade
from src import DATA_DIR


logger: LogFacade = LogFacade.get_logger("rule_engine1")


class RuleEngine1(BaseStrategy):
    """ Hull Moving avergae strategy """
    STRATEGY_CODE: str = "strategy1"
    CE_PREMIUM: float = 8000
    SL_PERCENT: float = 50
    QUANTITY: int = 50

    def __init__(self, dry_run: bool = False):
        super(RuleEngine1, self).__init__(dry_run=dry_run)
        self._redis: RedisBackend = RedisBackend()
        self._fyers_api: FyersApi = FyersApi()
        self._entry_instrument: Optional[Instrument] = None
        self._entry_instrument_sl: Optional[float] = None
        self._signal: Optional[Dict[str, Any]] = None
        # Store pre market and post market signal
        self._signal_queue: deque = deque()

    def setup(self) -> None:
        self._redis.connect()
        self._redis.subscribe(RuleEngine1.STRATEGY_CODE)
        if self.clean_up_flag:
            logger.info(f"Clean up flag is set")
            self.clean_up()
        self._fyers_api.setup()
        # If entry instrument pickle file exist, load the instrument from the file
        self._read_entry_instrument()
        # If signal queue pickle file exist, load the signals from the file
        self._read_signal_queue()

    def closeup(self) -> None:
        """ Save required data to file """
        if self._signal_queue:
            logger.info(f"Saving signal queue to {self.signal_queue_file} file")
            self._write_signal_queue()

    def clean_up(self) -> None:
        """ Cleaning up the old data """
        logger.info(f"Cleaning up old saved data")
        self.signal_queue_file.unlink(missing_ok=True)
        self.entry_instrument_file.unlink(missing_ok=True)

    def entry(self) -> None:
        """ Entry logic """
        if self._entry_instrument is not None:
            logger.warning(f"An entry has already been taken. Ignoring this entry signal.")
            return None
        self._entry_instrument = self._get_entry_instrument()
        # Check if per lot CE premium is more than 8000
        if not self._ce_premium_check():
            logger.info(
                f"Price for symbol {self._entry_instrument.symbol} is "
                f"{self._entry_instrument.price}"
            )
            logger.info(f"CE premium per lot exceeds 8000. Ignoring this signal.")
            self._entry_instrument = None
            return None
        logger.info(
            f"Entry taken for symbol {self._entry_instrument.symbol} at "
            f"{self._entry_instrument.entry} at price {self._entry_instrument.price}"
        )
        action = OrderAction.BUY if self._entry_instrument.action.upper() == "BUY" \
            else OrderAction.SELL
        logger.info(
            f"Placing {self._entry_instrument.action.upper()} market order for "
            f"{self._entry_instrument.symbol} with lot size {self._entry_instrument.lot_size}"
        )
        if self.dry_run:
            logger.info("Ignoring placing actual BUY order as dry-run mode is used")
        else:
            self._entry_instrument.order_id = self._fyers_api.place_cnc_market_order(
                symbol=self._entry_instrument.symbol_code,
                qty=self._entry_instrument.lot_size * RuleEngine1.QUANTITY,
                action=action
            )
        self._subscribe_live_market_data()
        # Save the entry instrument to a file after placing the order
        self._write_entry_instrument()
        self._entry_instrument_sl = 0

    def exit(self, *, sl_hit_exit: bool = False, expiry_exit: bool = False) -> None:
        """ Exit logic """
        if self._entry_instrument is None:
            logger.warning(f"No open position. Ignoring this exit signal.")
            return None
        market_depth = self._fyers_api.get_market_depth(self._entry_instrument.symbol_code)
        entry_instrument_ltp = market_depth['ltp']
        if sl_hit_exit:
            lot_size = self._entry_instrument.lot_size
            logger.info(
                f"Exiting lot size {lot_size} for symbol {self._entry_instrument.symbol} due "
                f"to SL hit at price {entry_instrument_ltp}. SL price was "
                f"{self._entry_instrument_sl}"
            )
        elif expiry_exit:
            lot_size = self._entry_instrument.lot_size
            logger.info(
                f"Exiting lot size {lot_size} for symbol {self._entry_instrument.symbol} due "
                f"to expiry at price {entry_instrument_ltp}. "
            )
        else:
            lot_size = self._signal["contract_size"]
            logger.info(
                f"Exiting {self._signal['action']} lot size {lot_size} for symbol "
                f"{self._entry_instrument.symbol} at {self._signal['timestamp']} at price "
                f"{entry_instrument_ltp}"
            )
        self._entry_instrument.lot_size -= lot_size
        action = OrderAction.SELL if self._entry_instrument.action.upper() == "BUY" \
            else OrderAction.BUY
        action_str = "BUY" if action.value == 1 else "SELL"
        logger.info(
            f"Placing {action_str} market order for {self._entry_instrument.symbol} with lot "
            f"size {lot_size}"
        )
        if self.dry_run:
            logger.info("Ignoring placing actual SELL order as dry-run mode is used")
        else:
            self._fyers_api.place_cnc_market_order(
                symbol=self._entry_instrument.symbol_code,
                qty=lot_size * RuleEngine1.QUANTITY,
                action=action
            )
        if self._entry_instrument.lot_size:
            # Update the entry_instrument in the file
            self._write_entry_instrument()
        else:
            # All the lots are exited
            self._entry_instrument = None
            self._entry_instrument_sl = 0
            self.entry_instrument_file.unlink(missing_ok=True)

    def execute(self) -> None:
        """ main function which will run forever and execute the strategy in an infinite loop """
        logger.info(f"Setting up the strategy {RuleEngine1.STRATEGY_CODE}")
        self.setup()
        logger.info(f"Starting execution of strategy {RuleEngine1.STRATEGY_CODE}")
        super(RuleEngine1, self).execute()
        while True:
            self._signal = self.get_tradingview_signal()
            now = istnow()
            if self.is_market_hour(now):
                # First process all the signals in signal queue
                if self._signal_queue:
                    self._process_signal_queue()
                if self._signal is None:
                    # If there is any active position
                    if self._entry_instrument is not None:
                        if self._sl_hit():
                            self.exit(sl_hit_exit=True)
                        if self._expiry_time_reach(now):
                            self.exit(expiry_exit=True)
                else:
                    # Signal from tradingview
                    if self.is_entry_signal(self._signal):
                        self.entry()
                    else:
                        self.exit()
            else:
                if self._signal is not None:
                    # After market close or pre market logic
                    logger.info(f"Signal received outside market hour. Adding to queue.")
                    self._signal_queue.append(self._signal)
            if self.trading_session_ends(now):
                logger.info(f"Trading session ends for the day.")
                self.closeup()
                return None

    def _process_signal_queue(self):
        """ Process all the signals in the signal queue """
        logger.info(f"Processing signal queue")
        while self._signal_queue:
            self._signal = self._signal_queue.popleft()
            logger.info(f"Signal from signal queue: {self._signal}")
            if self.is_entry_signal(self._signal):
                self.entry()
            else:
                self.exit()
        # Remove the file after processing the signals
        self.signal_queue_file.unlink(missing_ok=True)

    def get_tradingview_signal(self) -> Optional[Dict[str, Any]]:
        """ Check for any trading view signal """
        # get_message should only be called after subscribing to a channel
        message: str = self._redis.get_message()
        signal: Optional[Dict[str, Any]] = None
        if message:
            logger.info(f"Signal received from TradingView: {message}")
            signal = json.loads(message)
            # Signal datetime from tradingview will be in UTC
            signal["timestamp"] = datetime.datetime.fromisoformat(
                signal["timestamp"].strip("Z")
            )
            signal["timestamp"] = utc2ist(signal["timestamp"])
        return signal

    def _get_entry_instrument(self) -> Instrument:
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
        market_depth = self._fyers_api.get_market_depth(symbol_details["symbol_code"])
        action = "BUY" if self._signal["option_type"] == "CE" else "SELL"
        return Instrument(
            symbol=symbol_details["symbol"],
            symbol_code=symbol_details["symbol_code"],
            action=action,
            lot_size=self._signal["contract_size"],
            expiry=expiry,
            exchange_code=0,
            exchange=exchange,
            code=symbol_details["code"],
            option_type=self._signal["option_type"],
            strike=strike_price,
            entry=self._signal["timestamp"],
            price=market_depth["ltp"],
            order_id=None
        )

    def get_current_week_expiry(self, dt: datetime.date) -> datetime.date:
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
                self._subscribe_live_market_data()

    def _write_signal_queue(self):
        """ Save the signal queue to file """
        with open(self.signal_queue_file, mode="wb") as fp_:
            pickle.dump(self._signal_queue, fp_)

    def _read_signal_queue(self):
        """ Read signal queue from file """
        if self.signal_queue_file.is_file():
            with open(self.signal_queue_file, mode="rb") as fp_:
                self._signal_queue = pickle.load(fp_)

    def _ce_premium_check(self):
        """ CE premium check. If premium is more than 8000, don't take the trade """
        # 8000 premium check is per lot. Nifty 1 lot is 50 qty
        if self._entry_instrument.option_type == "CE" and \
                self._entry_instrument.price * RuleEngine1.QUANTITY > RuleEngine1.CE_PREMIUM:
            return False
        return True

    def _sl_hit(self) -> bool:
        """ Check if SL hits """
        self._calculate_entry_instrument_sl()
        entry_instrument_ltp = self._fyers_api.fyers_market_data.get_price(
            self._entry_instrument.symbol_code
        )
        if entry_instrument_ltp is None:
            # logger.warning(
            #     f"No live market data for symbol {self._entry_instrument.symbol} "
            #     f"for checking SL"
            # )
            # As we the live market data for this symbol and it is not available, subscribe for
            # the symbol so that we will start getting the live market data.
            self._subscribe_live_market_data()
        else:
            ce_sl_hit = self._entry_instrument.option_type == "CE" and \
                        entry_instrument_ltp < self._entry_instrument_sl
            pe_sl_hit = self._entry_instrument.option_type == "PE" and \
                        entry_instrument_ltp > self._entry_instrument_sl
            if ce_sl_hit or pe_sl_hit:
                return True
        return False

    def _calculate_entry_instrument_sl(self):
        if self._entry_instrument.option_type == "CE":
            # Its a long trade. So Stop Loss will be below the long price
            self._entry_instrument_sl = round(
                (100 - RuleEngine1.SL_PERCENT) * self._entry_instrument.price / 100,
                2
            )
        else:
            # Its a short trade. So Stop Loss will be above the short price
            self._entry_instrument_sl = round(
                (100 + RuleEngine1.SL_PERCENT) * self._entry_instrument.price / 100,
                2
            )
        # logger.info(f"SL price for the entry instrument is {self._entry_instrument_sl}")

    def _expiry_time_reach(self, now: datetime.datetime) -> bool:
        """ Return true if the now is expiry day 3:25 pm else false """
        expiry_date = self.get_current_week_expiry(now.date())
        expiry_time = datetime.time(hour=15, minute=25)
        expiry = datetime.datetime.combine(expiry_date, expiry_time)
        # Make the expiry timezone aware
        expiry = make_ist_aware(expiry)
        if now >= expiry:
            return True
        return False

    def _subscribe_live_market_data(self):
        """ Subscribe to live market data """
        # Subscribe the instrument for live market data
        # logger.info(f"Subscribing {self._entry_instrument.symbol} to live market data ")
        self._fyers_api.fyers_market_data.subscribe([self._entry_instrument.symbol_code])

    def _unsubscribe_live_market_data(self):
        """ Unsubscribe to live market data """
        # Subscribe the instrument for live market data
        logger.info(f"Unsubscribing {self._entry_instrument.symbol} to live market data ")
        self._fyers_api.fyers_market_data.unsubscribe([self._entry_instrument.symbol_code])

    @staticmethod
    def get_nearest_50_strike(index: float) -> int:
        """ Return the nearest 50 strike less than the index value """
        return int((index // 50) * 50)

    @property
    def entry_instrument_file(self) -> Path:
        return DATA_DIR / "entry_instrument.pkl"

    @property
    def signal_queue_file(self) -> Path:
        return DATA_DIR / "signal_queue.pkl"


if __name__ == "__main__":
    strategy = RuleEngine1()
    strategy.execute()
