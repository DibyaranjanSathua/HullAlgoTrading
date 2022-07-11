"""
File:           rule_engine3.py
Author:         Dibyaranjan Sathua
Created on:     04/07/22, 10:47 pm
"""
from typing import List, Tuple, Optional, Dict
import datetime
import time
from pathlib import Path
import pandas as pd
import sqlite3

from src.backtesting.base_backtesting import BaseBackTesting, Calendar
from src.backtesting.constant import SignalType, ExitType, EntryType
from src.backtesting.instrument import Instrument, InstrumentAction, CalendarInstrument
from src.backtesting.historical_data.db_api import DBApiPostgres
from src.backtesting.historical_data.models import HistoricalPrice
from src.backtesting.exception import BackTestingError
from src.backtesting.strategy_analysis import StrategyAnalysis, ConsecutiveWinLoss
from src.utils.logger import LogFacade


class RuleEngine3(BaseBackTesting, Calendar):
    """
    Rule Engine 3 backtesting. CE and PE calendar spread.
    For ETL signal, trade a PE calendar.
    For ETS signal, trade a CE calendar.
    For both CE and PE calendar, Buy current week strike and sell next week same strike.
    PE and CE calendar are mutually exclusive. At any given point of time, there should be
    either a CE or PE calendar and not both.
    """

    def __init__(
            self,
            config_file_path: str,
            input_excel_file_path: Optional[str] = None,
            output_excel_file_path: Optional[str] = None
    ):
        super(RuleEngine3, self).__init__(
            config_file_path=config_file_path,
            input_excel_file_path=input_excel_file_path,
            output_excel_file_path=output_excel_file_path
        )
        Calendar.__init__(self)
        # Key will be expiry and value will be list of historical prices
        self._historical_data: Dict[datetime.date, List[HistoricalPrice]] = dict()
        self._logger: LogFacade = LogFacade("rule_engine3")
        self._output_df: pd.DataFrame = pd.DataFrame(columns=[])
        self._trade_count: int = 0
        # Strategy Analysis
        self._strategy_analysis: StrategyAnalysis = StrategyAnalysis()
        self._strategy_analysis.consecutive_win_loss = ConsecutiveWinLoss()

    def execute(self) -> None:
        """ Execute backtesting """
        start_time = time.time()
        super(RuleEngine3, self).execute()
        self._strategy_analysis.initial_capital = self.config.get("initial_capital")
        self.process_input()
        self.save_df_to_excel(self._output_df, self.config["output_excel_file_path"])
        self._logger.info(f"Output excel is saved to {self.config['output_excel_file_path']}")
        execution_time = round(time.time() - start_time, 2)
        self._logger.info(f"Execution time: {execution_time} seconds")
        # Print strategy analysis
        self._strategy_analysis.print_analysis()

    def process_input(self) -> None:
        """ Process the input excel row by row """
        self._logger.info(
            f"Reading and processing input excel file {self.config['input_excel_file_path']}"
        )
        input_df = self.read_input_excel_to_df(Path(self.config["input_excel_file_path"]))
        for index, row in input_df.iterrows():
            # Signal type is entry and no entry has taken
            if row["Signal"] in (SignalType.ENTRY_LONG, SignalType.ENTRY_SHORT) \
                    and not self.is_entry_taken():
                self._logger.info(
                    f"Entry signal triggered at {row['Date/Time']} for price {row['Price']}"
                )
                lot_size = int(row["Contracts"])
                entry_datetime = self.get_market_hour_datetime(row["Date/Time"])
                current_expiry = self.get_valid_expiry(
                    self.get_current_week_expiry(entry_datetime.date())
                )
                next_expiry = self.get_valid_expiry(
                    self.get_next_week_expiry(entry_datetime.date())
                )
                self.entry(
                    price=row['Price'],
                    signal=row["Signal"],
                    entry_datetime=entry_datetime,
                    lot_size=lot_size,
                    current_expiry=current_expiry,
                    next_expiry=next_expiry
                )
            elif row["Signal"] in (SignalType.EXIT_LONG, SignalType.EXIT_SHORT) \
                    and self.is_entry_taken():
                self._logger.info(
                    f"Exit signal triggered at {row['Date/Time']} for price {row['Price']}"
                )
                # This is the signal exit datetime
                exit_datetime = self.get_market_hour_datetime(row["Date/Time"])
                lot_size = int(row["Contracts"])
                # Price is only used for logging.
                self.exit(price=row['Price'], exit_datetime=exit_datetime, lot_size=lot_size)

    def entry(
            self,
            price: float,
            signal: str,
            entry_datetime: datetime.datetime,
            lot_size: int,
            current_expiry: datetime.date,
            next_expiry: datetime.date
    ) -> None:
        """ Logic for entry trade. Either CE or PE calendar trade and not both. """
        self._logger.info(f"Entry taken at {entry_datetime}")
        # How much added to the price to get the strike price
        ce_strike = self.config.get("ce_strike", 0)
        pe_strike = self.config.get("pe_strike", 0)
        delta = pe_strike if signal == SignalType.ENTRY_LONG else ce_strike
        strike = self.get_nearest_100_strike(price, delta)
        # Get historical data for trading calendar. Fetch price data for both current
        # and next expiry for the same strike.
        self.fetch_historical_price(
            index=self.config["script"],
            strike=strike,
            option_type="PE" if signal == SignalType.ENTRY_LONG else "CE",
            current_expiry=current_expiry,
            next_expiry=next_expiry
        )
        ce_stop_loss = pe_stop_loss = None
        sl_check = self.config.get("SL_check")  # Get SL_check from config file
        if sl_check is not None:
            ce_stop_loss = sl_check["CE"]
            pe_stop_loss = sl_check["PE"]
        pe_entry_datetime = ce_entry_datetime = None
        pe_strike = ce_strike = None
        pe_current_week_price = ce_current_week_price = None
        pe_forward_week_price = ce_forward_week_price = None
        if signal == SignalType.ENTRY_LONG:
            self.trade_pe_calendar(
                strike=strike,
                entry_datetime=entry_datetime,
                lot_size=lot_size,
                current_expiry=current_expiry,
                next_expiry=next_expiry,
                stop_loss=pe_stop_loss
            )
            pe_entry_datetime = entry_datetime
            pe_strike = strike
            pe_current_week_price = self.PE_CALENDAR.current_week_instrument.price
            pe_forward_week_price = self.PE_CALENDAR.next_week_instrument.price
        else:
            self.trade_ce_calendar(
                strike=strike,
                entry_datetime=entry_datetime,
                lot_size=lot_size,
                current_expiry=current_expiry,
                next_expiry=next_expiry,
                stop_loss=ce_stop_loss
            )
            ce_entry_datetime = entry_datetime
            ce_strike = strike
            ce_current_week_price = self.CE_CALENDAR.current_week_instrument.price
            ce_forward_week_price = self.CE_CALENDAR.next_week_instrument.price
        self._trade_count += 1
        # Strategy Analysis
        if self._strategy_analysis.lot_size < lot_size:
            self._strategy_analysis.lot_size = lot_size
        self._strategy_analysis.total_trades += 1
        # Add to output dataframe
        df_data = {
            "Trade": self._trade_count,
            "Script": self.config["script"],
            "Expiry": current_expiry,
            "LotSize": lot_size,
            "EntryExitType": signal,
            "StrikePrice": price,
            "PEEntryExitTime": pe_entry_datetime,
            "PEStrike": pe_strike,
            "PECurrentWeekPrice": pe_current_week_price,
            "PECurrentWeekProfitLoss": 0,
            "PEForwardWeekPrice": pe_forward_week_price,
            "PEForwardWeekProfitLoss": 0,
            "NetPEProfitLoss": 0,
            "CEEntryExitTime": ce_entry_datetime,
            "CEStrike": ce_strike,
            "CECurrentWeekPrice": ce_current_week_price,
            "CECurrentWeekProfitLoss": 0,
            "CEForwardWeekPrice": ce_forward_week_price,
            "CEForwardWeekProfitLoss": 0,
            "NetCEProfitLoss": 0,
            "NetProfitLoss": 0
        }
        df = pd.DataFrame([list(df_data.values())], columns=list(df_data.keys()))
        self._output_df = pd.concat([self._output_df, df], axis=0)

    def exit(self, price: float, exit_datetime: datetime.datetime, lot_size: int) -> None:
        """ Logic for exit """
        quantity_per_lot = self.config["quantity_per_lot"]
        active_calendar = self.CE_CALENDAR or self.PE_CALENDAR
        assert active_calendar is not None, "Both CE and PE calendars are None inside exit function"
        current_expiry = active_calendar.current_week_instrument.expiry
        # Determine the actual exit time of the trade. If the signal exit datetime is more than
        # expiry then actual exit time is expiry.
        if exit_datetime.date() > current_expiry:
            exit_type = ExitType.EXPIRY_EXIT
            actual_exit_datetime = datetime.datetime.combine(
                current_expiry, datetime.time(hour=15, minute=25)
            )
        else:
            exit_type = ExitType.EXIT_SIGNAL
            actual_exit_datetime = exit_datetime
        # Stop loss settings
        sl_check = self.config.get("SL_check")  # Get SL_check from config file
        calendar_sl_hit = False
        pe_exit_datetime = ce_exit_datetime = None
        pe_strike = ce_strike = None
        pe_current_week_price = ce_current_week_price = None
        pe_current_week_pnl = ce_current_week_pnl = None
        pe_forward_week_price = ce_forward_week_price = None
        pe_forward_week_pnl = ce_forward_week_pnl = None
        pe_net_pnl = ce_net_pnl = None
        if sl_check is not None:
            ce_stop_loss = sl_check["CE"]
            pe_stop_loss = sl_check["PE"]
            calendar_sl_hit, sl_exit_price, sl_datetime = self.calendar_stop_loss_hit(
                calendar=active_calendar,
                exit_datetime=actual_exit_datetime,
                stop_loss=pe_stop_loss if active_calendar == self.PE_CALENDAR else ce_stop_loss,
                trailing_sl=self.config.get("trailing_sl", False)
            )
        if calendar_sl_hit:
            # When sl_hit we need to get the price of buy leg. We have exit the buy leg at the
            # same time.
            buy_leg_exit_price = self.get_instrument_exit_price(
                instrument=active_calendar.current_week_instrument,
                exit_datetime=sl_datetime
            )
            sell_leg_exit_price = sl_exit_price
            exit_type = ExitType.SL_EXIT
            actual_exit_datetime = sl_datetime
        else:
            # Normal exit
            # For this part the exit type is already calculated in top based on expiry date
            buy_leg_exit_price = self.get_instrument_exit_price(
                instrument=active_calendar.current_week_instrument,
                exit_datetime=actual_exit_datetime
            )
            sell_leg_exit_price = self.get_instrument_exit_price(
                instrument=active_calendar.next_week_instrument,
                exit_datetime=actual_exit_datetime
            )
        if active_calendar == self.PE_CALENDAR:
            pe_exit_datetime = actual_exit_datetime
            pe_strike = self.PE_CALENDAR.current_week_instrument.strike
            pe_current_week_price = buy_leg_exit_price
            # Buy leg pnl = exit price - entry price
            pe_current_week_pnl = (buy_leg_exit_price -
                                   self.PE_CALENDAR.current_week_instrument.price) * lot_size * quantity_per_lot
            pe_forward_week_price = sell_leg_exit_price
            # Sell leg pnl = entry price - exit price
            pe_forward_week_pnl = (self.PE_CALENDAR.next_week_instrument.price -
                                   sell_leg_exit_price) * lot_size * quantity_per_lot
            net_pnl = pe_net_pnl = pe_current_week_pnl + pe_forward_week_pnl
        else:
            ce_exit_datetime = actual_exit_datetime
            ce_strike = self.CE_CALENDAR.current_week_instrument.strike
            ce_current_week_price = buy_leg_exit_price
            # Buy leg pnl = exit price - entry price
            ce_current_week_pnl = (buy_leg_exit_price -
                                   self.CE_CALENDAR.current_week_instrument.price) * lot_size * quantity_per_lot
            ce_forward_week_price = sell_leg_exit_price
            # Sell leg pnl = entry price - exit price
            ce_forward_week_pnl = (self.CE_CALENDAR.next_week_instrument.price -
                                   sell_leg_exit_price) * lot_size * quantity_per_lot
            net_pnl = ce_net_pnl = ce_current_week_pnl + ce_forward_week_pnl

        # Strategy Analysis
        if net_pnl > 0:
            self._strategy_analysis.profit += net_pnl
            self._strategy_analysis.win_trades += 1
        else:
            self._strategy_analysis.loss += net_pnl
            self._strategy_analysis.loss_trades += 1
        self._strategy_analysis.consecutive_win_loss.compute(net_pnl)
        self._strategy_analysis.compute_equity_curve(net_pnl)
        # Add to output dataframe
        df_data = {
            "Trade": self._trade_count,
            "Script": self.config["script"],
            "Expiry": current_expiry,
            "LotSize": lot_size,
            "EntryExitType": exit_type,
            "StrikePrice": price,
            "PEEntryExitTime": pe_exit_datetime,
            "PEStrike": pe_strike,
            "PECurrentWeekPrice": pe_current_week_price,
            "PECurrentWeekProfitLoss": pe_current_week_pnl,
            "PEForwardWeekPrice": pe_forward_week_price,
            "PEForwardWeekProfitLoss": pe_forward_week_pnl,
            "NetPEProfitLoss": pe_net_pnl,
            "CEEntryExitTime": ce_exit_datetime,
            "CEStrike": ce_strike,
            "CECurrentWeekPrice": ce_current_week_price,
            "CECurrentWeekProfitLoss": ce_current_week_pnl,
            "CEForwardWeekPrice": ce_forward_week_price,
            "CEForwardWeekProfitLoss": ce_forward_week_pnl,
            "NetCEProfitLoss": ce_net_pnl,
            "NetProfitLoss": net_pnl
        }
        df = pd.DataFrame([list(df_data.values())], columns=list(df_data.keys()))
        self._output_df = pd.concat([self._output_df, df], axis=0)

        # Reduce the lot size
        active_calendar.current_week_instrument.lot_size -= lot_size
        active_calendar.next_week_instrument.lot_size -= lot_size
        assert active_calendar.current_week_instrument.lot_size == \
               active_calendar.next_week_instrument.lot_size, "Lot size for buy and sell legs " \
                                                              "are different"
        if self.PE_CALENDAR is not None and self.PE_CALENDAR.current_week_instrument.lot_size == 0:
            self.PE_CALENDAR = None
        if self.CE_CALENDAR is not None and self.CE_CALENDAR.current_week_instrument.lot_size == 0:
            self.CE_CALENDAR = None

    def fetch_historical_price(
            self,
            index: str,
            strike: int,
            option_type: str,
            current_expiry: datetime.date,
            next_expiry: datetime.date
    ) -> None:
        """ Get the list of historical price data for current and next week expiry """
        self._logger.info(
            f"Fetching historical data for {index} {strike} {option_type} for expiry "
            f"{current_expiry}"
        )
        self._historical_data[current_expiry] = DBApiPostgres.fetch_historical_data(
            session=self.session,   # Created in base class
            index=index,
            strike=strike,
            option_type=option_type,
            expiry=current_expiry
        )
        self._logger.info(
            f"Fetching historical data for {index} {strike} {option_type} for expiry {next_expiry}"
        )
        self._historical_data[next_expiry] = DBApiPostgres.fetch_historical_data(
            session=self.session,   # Created in base class
            index=index,
            strike=strike,
            option_type=option_type,
            expiry=next_expiry
        )

    def trade_ce_calendar(
            self,
            strike: int,
            entry_datetime: datetime.datetime,
            lot_size: int,
            current_expiry: datetime.date,
            next_expiry: datetime.date,
            stop_loss: Optional[float] = None
    ) -> None:
        """ Trade a CE calendar """

        # Buy CE current week expiry and sell next week expiry
        current_week_instrument = self.get_entry_instrument(
            entry_strike=strike,
            entry_datetime=entry_datetime,
            lot_size=lot_size,
            expiry=current_expiry,
            option_type="CE"
        )
        current_week_instrument.action = InstrumentAction.BUY
        next_week_instrument = self.get_entry_instrument(
            entry_strike=strike,
            entry_datetime=entry_datetime,
            lot_size=lot_size,
            expiry=next_expiry,
            option_type="CE"
        )
        next_week_instrument.action = InstrumentAction.SELL
        # Stop loss will be on SELL leg
        sl_price = None
        if stop_loss is not None:
            # Its a short trade. So Stop Loss will be above the short price
            sl_price = round(
                (100 + stop_loss) * next_week_instrument.price / 100, 2
            )
        next_week_instrument.sl_price = sl_price
        self.CE_CALENDAR = CalendarInstrument(
            current_week_instrument=current_week_instrument,
            next_week_instrument=next_week_instrument
        )

    def trade_pe_calendar(
            self,
            strike: int,
            entry_datetime: datetime.datetime,
            lot_size: int,
            current_expiry: datetime.date,
            next_expiry: datetime.date,
            stop_loss: Optional[float] = None
    ) -> None:
        """ Trade a PE calendar """
        # Buy CE current week expiry and sell next week expiry
        current_week_instrument = self.get_entry_instrument(
            entry_strike=strike,
            entry_datetime=entry_datetime,
            lot_size=lot_size,
            expiry=current_expiry,
            option_type="PE"
        )
        current_week_instrument.action = InstrumentAction.BUY
        next_week_instrument = self.get_entry_instrument(
            entry_strike=strike,
            entry_datetime=entry_datetime,
            lot_size=lot_size,
            expiry=next_expiry,
            option_type="PE"
        )
        next_week_instrument.action = InstrumentAction.SELL
        # Stop loss will be on SELL leg
        sl_price = None
        if stop_loss is not None:
            # Its a short trade. So Stop Loss will be above the short price
            sl_price = round(
                (100 + stop_loss) * next_week_instrument.price / 100, 2
            )
        next_week_instrument.sl_price = sl_price
        self.PE_CALENDAR = CalendarInstrument(
            current_week_instrument=current_week_instrument,
            next_week_instrument=next_week_instrument
        )

    def get_entry_instrument(
            self,
            entry_strike: int,
            entry_datetime: datetime.datetime,
            lot_size: int,
            expiry: datetime.date,
            option_type: str
    ) -> Instrument:
        symbol = f"{self.config['script']} {entry_strike} {option_type}"
        # Get the price for CE or PE strike at entry_datetime
        try:
            entry_price = self.get_historical_price_by_datetime(expiry=expiry, dt=entry_datetime)
        except BackTestingError:
            # Checking if data actually exist in database
            if self.is_data_missing(entry_strike, entry_datetime):
                msg = f"Data is missing for {symbol} at {entry_datetime} for expiry {expiry}"
                self._logger.error(msg)
                entry_price = 0
            else:
                msg = f"No price data found for {symbol} at {entry_datetime} for expiry {expiry}"
                self._logger.error(msg)
                raise BackTestingError(msg)

        return Instrument(
            symbol=symbol,
            lot_size=int(lot_size),
            entry=entry_datetime,
            expiry=expiry,
            option_type=option_type,
            strike=entry_strike,
            price=entry_price
        )

    def get_instrument_exit_price(
            self, instrument: Instrument, exit_datetime: datetime.datetime
    ) -> float:
        """ Return the exit price of the calendar's leg """
        try:
            return self.get_historical_price_by_datetime(
                expiry=instrument.expiry,
                dt=exit_datetime
            )
        except BackTestingError:
            # Checking if data actually exist in database
            if self.is_data_missing(instrument.strike, exit_datetime):
                msg = f"Data is missing for {instrument.symbol} at {exit_datetime} for expiry " \
                      f"{instrument.expiry}"
                self._logger.error(msg)
                return 0
            else:
                msg = f"No price data found for {instrument.symbol} at {exit_datetime} for expiry " \
                      f"{instrument.expiry}"
                self._logger.error(msg)
                raise BackTestingError(msg)

    def get_historical_price_by_datetime(
            self, expiry: datetime.date, dt: datetime.datetime
    ) -> float:
        """ Filter out the price by datetime """
        historical_data = self._historical_data[expiry]
        price_data = next(
            (
                x
                for x in historical_data
                # Some of the minutes data is not available. So we take the next minute available
                # data for the same day
                if x.ticker_datetime.date() == dt.date() and x.ticker_datetime >= dt
            ),
            None
        )
        if price_data is None:
            raise BackTestingError(f"price data is missing for {dt} for {expiry}")
        return price_data.close

    def get_historical_price_range_data(
            self,
            start_datetime: datetime.datetime,
            end_datetime: datetime.datetime,
            expiry: datetime.date
    ) -> List[HistoricalPrice]:
        """
        Returns price range minute by minute data between start datetime and end datetime.
        This is use for checking SL hit. Start datetime is entry datetime and end datetime is
        actual exit datetime.
        """
        historical_data = self._historical_data[expiry]
        return [
            x
            for x in historical_data
            if start_datetime < x.ticker_datetime < end_datetime
        ]

    def calendar_stop_loss_hit(
            self,
            calendar: CalendarInstrument,
            exit_datetime: datetime.datetime,
            stop_loss: float,
            trailing_sl: bool = True
    ) -> Tuple[bool, Optional[float], Optional[datetime.datetime]]:
        """
        Check if SL hit for calendar. Check SL on the Sell Leg.
        Return a tuple with first element a boolean indicate if SL hit or not. Second element is
        the price at which the SL hit. Third element is the time SL hit.
        """
        price_data = self.get_historical_price_range_data(
            start_datetime=calendar.next_week_instrument.entry,
            end_datetime=exit_datetime,
            expiry=calendar.next_week_instrument.expiry
        )
        for data in price_data:
            # If trailing_sl is True, change the sl price
            if trailing_sl:
                # Its a short trade. So Stop Loss will be above the short price
                sl_price = round((100 + stop_loss) * data.close / 100, 2)
                # Trailing stop loss. If the current sl_price is less than previous sl_price,
                # trail it. This is a short trade. So when price is going down, we need to trail
                # the stop loss. And when price is going down, the new sl will be less then the
                # prev sl.
                # calendar.next_week_instrument.sl_price is calculated during the entry
                if sl_price < calendar.next_week_instrument.sl_price:
                    calendar.next_week_instrument.sl_price = sl_price
            # Short trade. If price goes above instrument_sl_price, SL hit
            if data.close > calendar.next_week_instrument.sl_price:
                return True, data.close, data.ticker_datetime
        return False, None, None

    @property
    def strategy_analysis(self) -> StrategyAnalysis:
        return self._strategy_analysis
