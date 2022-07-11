"""
File:           hull_ma_strategy.py
Author:         Dibyaranjan Sathua
Created on:     16/04/22, 12:06 pm
"""
from typing import List, Tuple, Optional
import datetime
import time
from pathlib import Path
import pandas as pd
import sqlite3

from src.backtesting.base_backtesting import BaseBackTesting, PairInstrument
from src.backtesting.constant import SignalType, ExitType, EntryType
from src.backtesting.instrument import Instrument
from src.backtesting.historical_data.db_api import DBApiPostgres
from src.backtesting.historical_data.models import HistoricalPrice
from src.backtesting.exception import BackTestingError
from src.backtesting.strategy_analysis import StrategyAnalysis, ConsecutiveWinLoss
from src.utils.logger import LogFacade


class HullMABackTesting(BaseBackTesting, PairInstrument):
    """ Hull moving average backtesting """

    def __init__(
            self,
            config_file_path: str,
            input_excel_file_path: Optional[str] = None,
            output_excel_file_path: Optional[str] = None
    ):
        super(HullMABackTesting, self).__init__(
            config_file_path=config_file_path,
            input_excel_file_path=input_excel_file_path,
            output_excel_file_path=output_excel_file_path
        )
        PairInstrument.__init__(self)
        self._ce_historical_data: List[HistoricalPrice] = []
        self._pe_historical_data: List[HistoricalPrice] = []
        self._logger: LogFacade = LogFacade("hull_ma_backtesting")
        self._output_df: pd.DataFrame = pd.DataFrame(columns=[])
        self._trade_count: int = 0
        # Position entry datetime (entry signal datetime)
        self._entry_datetime: Optional[datetime.datetime] = None
        self._active_instrument_expiry: Optional[datetime.date] = None
        self._entry_strike: Optional[int] = None
        # Below two variables are used when we close the trade at day end and open the trade
        # again next day. Used when close_position_day_end is set to true in config
        self._trade_ce_next_day: bool = False
        self._trade_pe_next_day: bool = False
        # SL for the instrument
        self._ce_sl: Optional[float] = None
        self._pe_sl: Optional[float] = None
        # Strategy analysis
        self._pe_strategy_analysis: StrategyAnalysis = StrategyAnalysis()
        self._pe_strategy_analysis.consecutive_win_loss = ConsecutiveWinLoss()
        self._ce_strategy_analysis: StrategyAnalysis = StrategyAnalysis()
        self._ce_strategy_analysis.consecutive_win_loss = ConsecutiveWinLoss()

    def process_input(self):
        """ Process the input excel row by row """
        self._logger.info(
            f"Reading and processing input excel file {self.config['input_excel_file_path']}"
        )
        input_df = self.read_input_excel_to_df(Path(self.config["input_excel_file_path"]))
        use_pe_atm_strike = False
        use_ce_atm_strike = False
        close_position_day_end = self.config.get("close_position_day_end", False)
        if close_position_day_end:
            use_pe_atm_strike = self.config.get("use_pe_atm_strike", False)
            use_ce_atm_strike = self.config.get("use_ce_atm_strike", False)
        for index, row in input_df.iterrows():
            # Signal type is entry and no entry has taken
            if row["Signal"] == SignalType.ENTRY and not self.is_entry_taken():
                self._logger.info(
                    f"Entry signal triggered at {row['Date/Time']} for price {row['Price']}"
                )
                self._entry_datetime = self.get_market_hour_datetime(row["Date/Time"])
                self._entry_strike = self.get_entry_strike(row["Price"])
                self._active_instrument_expiry = self.get_expiry(
                    self._entry_datetime.date()
                )
                lot_size = int(row["Contracts"])
                self.entry(
                    ce_entry_strike=self._entry_strike,
                    pe_entry_strike=self._entry_strike,
                    entry_datetime=self._entry_datetime,
                    lot_size=lot_size,
                    expiry=self._active_instrument_expiry
                )
                if close_position_day_end:
                    self._trade_pe_next_day = True
                    self._trade_ce_next_day = True
                # This is an actual entry signal. We will calculate the SL for this instrument and
                # use this same SL for everyday exit and next day entry instrument.
                # SL will be calculated only once during the actual entry. The same SL will be used
                # even if we are exiting the position everyday and taking a new position next day.
                # check instrument_stop_loss_hit() function.
                self._ce_sl = None
                self._pe_sl = None
            elif row["Signal"] == SignalType.EXIT and self.is_entry_taken():
                self._logger.info(
                    f"Exit signal triggered at {row['Date/Time']} for price {row['Price']}"
                )
                # This is the signal exit datetime
                exit_datetime = self.get_market_hour_datetime(row["Date/Time"])
                lot_size = int(row["Contracts"])
                # Mode in which we close all the position at day end and take the same position
                # again on next day
                if close_position_day_end:
                    # Close the active position at day end and again take the position next day
                    self.soft_entry_exit(
                        exit_datetime=exit_datetime,
                        lot_size=lot_size,
                        use_pe_atm_strike=use_pe_atm_strike,
                        use_ce_atm_strike=use_ce_atm_strike,
                    )
                else:
                    self.exit(exit_datetime=exit_datetime, lot_size=lot_size)

    def fetch_historical_price(
            self, index: str, ce_strike: int, pe_strike: int, expiry: datetime.date
    ) -> None:
        """ Get the list of CE historical price data """
        self._logger.info(
            f"Fetching historical data for {index} {ce_strike} CE for expiry {expiry}"
        )
        self._ce_historical_data = DBApiPostgres.fetch_historical_data(
            session=self.session,   # Created in base class
            index=index,
            strike=ce_strike,
            option_type="CE",
            expiry=expiry
        )
        self._logger.info(
            f"Fetching historical data for {index} {pe_strike} PE for expiry {expiry}"
        )
        self._pe_historical_data = DBApiPostgres.fetch_historical_data(
            session=self.session,   # Created in base class
            index=index,
            strike=pe_strike,
            option_type="PE",
            expiry=expiry
        )

    def get_historical_price_by_datetime(self, option_type: str, dt: datetime.datetime) -> float:
        """ Filter out the price by datetime """
        if option_type == "CE":
            historical_data = self._ce_historical_data
        else:
            historical_data = self._pe_historical_data

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
            raise BackTestingError(f"price data is missing for {dt}")
        return price_data.close

    def get_ce_entry_instrument(
            self,
            entry_strike: int,
            entry_datetime: datetime.datetime,
            lot_size: int,
            expiry: datetime.date,

    ) -> Instrument:
        symbol = f"{self.config['script']} {entry_strike} CE"
        # Get the price for CE strike at entry_datetime
        try:
            ce_entry_price = self.get_historical_price_by_datetime(
                option_type="CE", dt=entry_datetime
            )
        except BackTestingError:
            # Checking if data actually exist in database
            if self.is_data_missing(entry_strike, entry_datetime):
                msg = f"Data is missing for {symbol} at {entry_datetime} for expiry {expiry}"
                self._logger.error(msg)
                ce_entry_price = 0
            else:
                msg = f"No price data found for {symbol} at {entry_datetime} for expiry {expiry}"
                self._logger.error(msg)
                raise BackTestingError(msg)

        return Instrument(
            symbol=symbol,
            lot_size=int(lot_size),
            entry=entry_datetime,
            expiry=expiry,
            option_type="CE",
            strike=entry_strike,
            price=ce_entry_price
        )

    def get_pe_entry_instrument(
            self,
            entry_strike: int,
            entry_datetime: datetime.datetime,
            lot_size: int,
            expiry: datetime.date,
    ) -> Instrument:
        symbol = f"{self.config['script']} {entry_strike} PE"
        # Get the price for PE strike at entry_datetime
        try:
            pe_entry_price = self.get_historical_price_by_datetime(
                option_type="PE", dt=entry_datetime
            )
        except BackTestingError:
            # Checking if data actually exist in database
            if self.is_data_missing(entry_strike, entry_datetime):
                msg = f"Data is missing for {symbol} at {entry_datetime} for expiry {expiry}"
                self._logger.error(msg)
                pe_entry_price = 0
            else:
                msg = f"No price data found for {symbol} at {entry_datetime} for expiry {expiry}"
                self._logger.error(msg)
                raise BackTestingError(msg)

        return Instrument(
            symbol=symbol,
            lot_size=int(lot_size),
            entry=entry_datetime,
            expiry=expiry,
            option_type="PE",
            strike=entry_strike,
            price=pe_entry_price
        )

    def trade_ce_instrument(self, ce_instrument: Instrument) -> bool:
        """
        If ce_premium_check is defined in config file, then check if the ce premium is exceeding
        the amount mentioned in config file. Skip ce trade if premium amount exceeds.
        Returns True when ce premium < premium in config file or ce_premium_check is not defined or
        set to null in json file.
        Returns False when ce premium > premium in config file
        """
        premium_check = self.config.get("ce_premium_check")
        if premium_check is not None:
            # Premium in config file is per lot. So multiply it by lot_size
            ce_instrument_premium = ce_instrument.price * ce_instrument.lot_size * \
                                    self.config["quantity_per_lot"]
            if ce_instrument_premium > premium_check["premium"] * ce_instrument.lot_size:
                return False
        return True

    def get_historical_price_range_data(
            self,
            start_datetime: datetime.datetime,
            end_datetime: datetime.datetime,
            option_type: str
    ) -> List[HistoricalPrice]:
        """
        Returns price range minute by minute data between start datetime and end datetime.
        This is use for checking SL hit. Start datetime is entry datetime and end datetime is
        actual exit datetime.
        """
        if option_type == "CE":
            historical_data = self._ce_historical_data
        else:
            historical_data = self._pe_historical_data

        return [
            x
            for x in historical_data
            if start_datetime < x.ticker_datetime < end_datetime
        ]

    def instrument_stop_loss_hit(
            self, instrument: Instrument, stop_loss: float, exit_datetime: datetime.datetime
    ) -> Tuple[bool, Optional[float], Optional[datetime.datetime]]:
        """
        Check if SL hit for an instrument.
        Return a tuple with first element a boolean indicate if SL hit or not. Second element is
        the price at which the SL hit. Third element is the time SL hit.
        stop_loss is in percentage like 20%, 25%.
        """
        price_data = self.get_historical_price_range_data(
            start_datetime=instrument.entry,
            end_datetime=exit_datetime,
            option_type=instrument.option_type
        )
        # For everyday entry exit, we will not calculate the SL on everyday new entry.
        # We will keep the same SL that we use for the actual entry signal
        if instrument.option_type == "CE":
            if self._ce_sl is None:
                # self.ce_sl is None means we are calculating the SL for the first time for this
                # entry instrument
                # Its a long trade. So Stop Loss will be below the long price
                instrument_sl_price = 0 if stop_loss == 100 \
                    else round((100 - stop_loss) * instrument.price / 100, 2)
            else:
                instrument_sl_price = self._ce_sl
        else:
            if self._pe_sl is None:
                # self.pe_sl is None means we are calculating the SL for the first time for this
                # entry instrument
                # Its a short trade. So Stop Loss will be above the short price
                instrument_sl_price = round((100 + stop_loss) * instrument.price / 100, 2)
            else:
                instrument_sl_price = self._pe_sl
        for data in price_data:
            # Long trade. If price goes below instrument_sl_price, SL hit
            if instrument.option_type == "CE" and data.close < instrument_sl_price:
                return True, data.close, data.ticker_datetime
            # Short trade. If price goes above instrument_sl_price, SL hit
            elif instrument.option_type == "PE" and data.close > instrument_sl_price:
                return True, data.close, data.ticker_datetime
        return False, None, None

    def instrument_take_profit_hit(
            self, instrument: Instrument, profit_percent: float, exit_datetime: datetime.datetime
    ) -> Tuple[bool, Optional[float], Optional[datetime.datetime]]:
        """
        Check if target profit hit for an instrument.
        Return a tuple with first element a boolean indicate if target profit hit or not.
        Second element is the price at which the profit hit. Third element is the time profit hit.
        profit_percent is in percentage like 20%, 25%.
        """
        price_data = self.get_historical_price_range_data(
            start_datetime=instrument.entry,
            end_datetime=exit_datetime,
            option_type=instrument.option_type
        )
        if instrument.option_type == "CE":
            # Its a long trade. So target profit will be above the long price
            instrument_target_price = round((100 + profit_percent) * instrument.price / 100, 2)
        else:
            # Its a short trade. So target profit will be below the short price
            instrument_target_price = 0 if profit_percent == 100 \
                else round((100 - profit_percent) * instrument.price / 100, 2)
        for data in price_data:
            # Long trade. If price goes above instrument_target_price, target profit hit
            if instrument.option_type == "CE" and data.close >= instrument_target_price:
                return True, data.close, data.ticker_datetime
            # Short trade. If price goes below instrument_target_price, target profit hit
            elif instrument.option_type == "PE" and data.close <= instrument_target_price:
                return True, data.close, data.ticker_datetime
        return False, None, None

    def entry(
            self,
            ce_entry_strike: int,
            pe_entry_strike: int,
            entry_datetime: datetime.datetime,
            lot_size: int,
            expiry: datetime.date,
            soft_entry: bool = False,
    ) -> None:
        """
        Logic for trade entry. soft_entry is used when we close the position at dat end and open
        the same position again next day.
        """
        # Get historical data for both PE and CE for entry strike and expiry
        self.fetch_historical_price(
            index=self.config["script"],
            ce_strike=ce_entry_strike,
            pe_strike=pe_entry_strike,
            expiry=expiry
        )
        # Strategy analysis
        if self._pe_strategy_analysis.lot_size < lot_size:
            self._pe_strategy_analysis.lot_size = lot_size
        if self._ce_strategy_analysis.lot_size < lot_size:
            self._ce_strategy_analysis.lot_size = lot_size
        # Indicate if we close position at day end and take it again next day
        close_position_day_end = self.config.get("close_position_day_end", False)
        # Take CE entry when soft_entry is False (actual signal entry) or when soft_entry is True
        # and self._trade_ce_next_day is True which means SL not hit on previous day
        # Soft entry is true when we close an entry on the day end and take the same entry next day
        ce_instrument_price = 0
        ce_entry_type = ""
        if not soft_entry or (soft_entry and self._trade_ce_next_day):
            self.CE_ENTRY_INSTRUMENT = self.get_ce_entry_instrument(
                entry_strike=ce_entry_strike,
                entry_datetime=entry_datetime,
                lot_size=lot_size,
                expiry=expiry
            )
            ce_entry_type = EntryType.SOFT_ENTRY if soft_entry else EntryType.ENTRY_SIGNAL
            ce_instrument_price = self.CE_ENTRY_INSTRUMENT.price
            # Check CE instrument premium
            if not self.trade_ce_instrument(self.CE_ENTRY_INSTRUMENT):
                self._logger.info(
                    f"CE premium for {self.CE_ENTRY_INSTRUMENT.symbol} exceeds the premium "
                    f"specified in config file. Ignoring CE trading."
                )
                self.CE_ENTRY_INSTRUMENT = None
                ce_entry_type = ""
                if close_position_day_end:
                    # If close_position_day_end is enabled and at the actual entry signal we don't
                    # take an entry for CE due to premium check, we shouldn't take any entry
                    # next day also. We will skip the whole entry exit signal.
                    self._trade_ce_next_day = False
        # Take PE entry when soft_entry is False (actual signal entry) or when soft_entry is True
        # and self._trade_ce_next_day is True which means SL not hit on previous day
        # Soft entry is true when we close an entry on the day end and take the same entry next day
        pe_instrument_price = 0
        pe_entry_type = ""
        if not soft_entry or (soft_entry and self._trade_pe_next_day):
            self.PE_ENTRY_INSTRUMENT = self.get_pe_entry_instrument(
                entry_strike=pe_entry_strike,
                entry_datetime=entry_datetime,
                lot_size=lot_size,
                expiry=expiry
            )
            pe_entry_type = EntryType.SOFT_ENTRY if soft_entry else EntryType.ENTRY_SIGNAL
            pe_instrument_price = self.PE_ENTRY_INSTRUMENT.price
        # Check if there is any entry for either PE or CE. If no entry, return from this function.
        # Normally when we program enters this function, then we are supposed to take an entry
        # for either CE or PE. But consider a scenario when for a soft entry, PL SL was hit previous
        # day and we comes to this function for CE entry. But CE entry is also skipped due to
        # premium check. So we are not taking any trade at all.
        if self.CE_ENTRY_INSTRUMENT is None and self.PE_ENTRY_INSTRUMENT is None:
            return None
        self._trade_count += 1
        # Strategy Analysis
        if self.PE_ENTRY_INSTRUMENT:
            self._pe_strategy_analysis.total_trades += 1
        if self.CE_ENTRY_INSTRUMENT:
            self._ce_strategy_analysis.total_trades += 1
        df_data = {
            "Trade": self._trade_count,
            "Script": self.config["script"],
            "Expiry": expiry,
            "LotSize": self.PE_ENTRY_INSTRUMENT.lot_size if self.PE_ENTRY_INSTRUMENT is not None
            else self.CE_ENTRY_INSTRUMENT.lot_size,
            "PEStrike": pe_entry_strike,
            "PEEntryExitTime": entry_datetime,
            "PESell": pe_instrument_price,
            "PEProfitLoss": 0,
            "PEEntryExitType": pe_entry_type,
            "CEStrike": ce_entry_strike,
            "CEEntryExitTime": entry_datetime,
            "CEBuy": ce_instrument_price,
            "CEProfitLoss": 0,
            "CEEntryExitType": ce_entry_type
        }
        df = pd.DataFrame([list(df_data.values())], columns=list(df_data.keys()))
        self._output_df = pd.concat([self._output_df, df], axis=0)

    def exit(
            self, exit_datetime: datetime.datetime, lot_size: int, soft_exit: bool = False
    ) -> None:
        """
        Logic for trade exit. soft_exit is used when we close the position at dat end and open
        the same position again next day.
        """
        quantity_per_lot = self.config["quantity_per_lot"]
        # We need one active instrument to find the strike and lot size
        active_instrument = self.PE_ENTRY_INSTRUMENT or self.CE_ENTRY_INSTRUMENT
        assert active_instrument is not None, "Both PE and CE instrument is None inside " \
                                              "exit function"
        expiry = active_instrument.expiry
        # Determine the actual exit time of the trade. If the signal exit datetime is more than
        # expiry then actual exit time is expiry.
        if exit_datetime.date() > expiry:
            ce_exit_type = pe_exit_type = ExitType.EXPIRY_EXIT
            ce_actual_exit_datetime = pe_actual_exit_datetime = datetime.datetime.combine(
                expiry, datetime.time(hour=15, minute=29)
            )
        else:
            ce_exit_type = pe_exit_type = ExitType.EXIT_SIGNAL
            ce_actual_exit_datetime = pe_actual_exit_datetime = exit_datetime

        # Exiting the trade at day end
        if soft_exit:
            ce_exit_type = pe_exit_type = ExitType.SOFT_EXIT

        # Variables to track if the instrument is exited due to SL
        ce_instrument_sl_hit = False
        ce_instrument_profit_hit = False
        pe_instrument_sl_hit = False
        pe_instrument_profit_hit = False
        sl_check = self.config.get("SL_check")      # Get SL_check from config file
        take_profit_check = self.config.get("take_profit_check")
        # Get CE exit price
        ce_exit_price = 0
        ce_profit_loss = 0
        if self.CE_ENTRY_INSTRUMENT is not None:
            # Check if SL exit mode is set in config file. If yes, check minute by minute data
            # to see if SL is getting hit
            if sl_check is not None:
                ce_instrument_sl_hit, ce_sl_exit_price, sl_datetime = self.instrument_stop_loss_hit(
                    instrument=self.CE_ENTRY_INSTRUMENT,
                    stop_loss=sl_check["CE"],
                    exit_datetime=ce_actual_exit_datetime
                )
            if take_profit_check is not None:
                ce_instrument_profit_hit, ce_profit_exit_price, profit_datetime = self.instrument_take_profit_hit(
                    instrument=self.CE_ENTRY_INSTRUMENT,
                    profit_percent=take_profit_check["CE"],
                    exit_datetime=ce_actual_exit_datetime
                )
            if ce_instrument_sl_hit:
                # This is True when SL check is ON and CE SL hits
                self._logger.info(f"SL hit for {self.CE_ENTRY_INSTRUMENT.symbol}")
                ce_exit_price = ce_sl_exit_price
                ce_actual_exit_datetime = sl_datetime
                ce_exit_type = ExitType.SL_EXIT
                # SL hit for CE. We shouldn't soft enter again the next day
                # when close_position_day_end is True
                if soft_exit:
                    self._trade_ce_next_day = False
            elif ce_instrument_profit_hit:
                # This is True when take profit check in ON and CE profit hits
                self._logger.info(f"Target profit hit for {self.CE_ENTRY_INSTRUMENT.symbol}")
                ce_exit_price = ce_profit_exit_price
                ce_actual_exit_datetime = profit_datetime
                ce_exit_type = ExitType.TAKE_PROFIT_EXIT
                # Take profit hit. We shouldn't soft enter again the next day
                # when close_position_day_end is True
                if soft_exit:
                    self._trade_ce_next_day = False
            else:
                # For this part the exit type is already calculated in top based on expiry date
                try:
                    ce_exit_price = self.get_historical_price_by_datetime(
                        option_type="CE", dt=ce_actual_exit_datetime
                    )
                except BackTestingError:
                    # Checking if data actually exist in database
                    if self.is_data_missing(self.CE_ENTRY_INSTRUMENT.strike, ce_actual_exit_datetime):
                        msg = f"Data is missing for {self.CE_ENTRY_INSTRUMENT.symbol} at " \
                              f"{ce_actual_exit_datetime} for expiry {expiry}"
                        self._logger.error(msg)
                        ce_exit_price = 0
                    else:
                        msg = f"No price data found for {self.CE_ENTRY_INSTRUMENT.symbol} at " \
                              f"{ce_actual_exit_datetime} for expiry {expiry}"
                        self._logger.error(msg)
                        raise BackTestingError(msg)
            ce_profit_loss = (ce_exit_price - self.CE_ENTRY_INSTRUMENT.price) * lot_size * \
                             quantity_per_lot
            self.CE_ENTRY_INSTRUMENT.lot_size -= lot_size
            # Strategy analysis
            if ce_profit_loss > 0:
                self._ce_strategy_analysis.profit += ce_profit_loss
                self._ce_strategy_analysis.win_trades += 1
            else:
                self._ce_strategy_analysis.loss += ce_profit_loss
                self._ce_strategy_analysis.loss_trades += 1
            self._ce_strategy_analysis.consecutive_win_loss.compute(ce_profit_loss)
            self._ce_strategy_analysis.compute_equity_curve(ce_profit_loss)
        else:
            if soft_exit:
                ce_exit_type = ExitType.NO_TRADE
                # For safe side, when CE instrument is None (no entry taken) and soft_exit is True
                # make self._trade_ce_next_day to ensure no soft entry taken next day
                self._trade_ce_next_day = False
            else:
                ce_exit_type = ExitType.CE_PREMIUM_EXIT

        # Get PE exit price
        pe_exit_price = 0
        pe_profit_loss = 0
        if self.PE_ENTRY_INSTRUMENT is not None:
            # Check if SL exit mode is set in config file. If yes, check minute by minute data
            # to see if SL is getting hit
            if sl_check is not None:
                pe_instrument_sl_hit, pe_sl_exit_price, sl_datetime = self.instrument_stop_loss_hit(
                    instrument=self.PE_ENTRY_INSTRUMENT,
                    stop_loss=sl_check["PE"],
                    exit_datetime=pe_actual_exit_datetime
                )
            if take_profit_check is not None:
                pe_instrument_profit_hit, pe_profit_exit_price, profit_datetime = self.instrument_take_profit_hit(
                    instrument=self.PE_ENTRY_INSTRUMENT,
                    profit_percent=take_profit_check["PE"],
                    exit_datetime=pe_actual_exit_datetime
                )
            if pe_instrument_sl_hit:
                # This is True when SL check is ON and CE SL hits
                self._logger.info(f"SL hit for {self.PE_ENTRY_INSTRUMENT.symbol}")
                pe_exit_price = pe_sl_exit_price
                pe_actual_exit_datetime = sl_datetime
                pe_exit_type = ExitType.SL_EXIT
                # SL hit for PE. We shouldn't soft enter again the next day
                # when close_position_day_end is True
                if soft_exit:
                    self._trade_pe_next_day = False
            elif pe_instrument_profit_hit:
                # This is True when take profit check in ON and PE profit hits
                self._logger.info(f"Target profit hit for {self.PE_ENTRY_INSTRUMENT.symbol}")
                pe_exit_price = pe_profit_exit_price
                pe_actual_exit_datetime = profit_datetime
                pe_exit_type = ExitType.TAKE_PROFIT_EXIT
                # Take profit hit for PE. We shouldn't soft enter again the next day
                # when close_position_day_end is True
                if soft_exit:
                    self._trade_pe_next_day = False
            else:
                try:
                    pe_exit_price = self.get_historical_price_by_datetime(
                        option_type="PE", dt=pe_actual_exit_datetime
                    )
                except BackTestingError:
                    # Checking if data actually exist in database
                    if self.is_data_missing(self.PE_ENTRY_INSTRUMENT.strike, pe_actual_exit_datetime):
                        msg = f"Data is missing for {self.PE_ENTRY_INSTRUMENT.symbol} at " \
                              f"{pe_actual_exit_datetime} for expiry {expiry}"
                        self._logger.error(msg)
                        pe_exit_price = 0
                    else:
                        msg = f"No price data found for {self.PE_ENTRY_INSTRUMENT.symbol} at " \
                              f"{pe_actual_exit_datetime} for expiry {expiry}"
                        self._logger.error(msg)
                        raise BackTestingError(msg)

            pe_profit_loss = (self.PE_ENTRY_INSTRUMENT.price - pe_exit_price) * lot_size * \
                             quantity_per_lot
            # Decrease the lot size from the entry instrument
            self.PE_ENTRY_INSTRUMENT.lot_size -= lot_size
            # Strategy analysis
            if pe_profit_loss > 0:
                self._pe_strategy_analysis.profit += pe_profit_loss
                self._pe_strategy_analysis.win_trades += 1
            else:
                self._pe_strategy_analysis.loss += pe_profit_loss
                self._pe_strategy_analysis.loss_trades += 1
            self._pe_strategy_analysis.consecutive_win_loss.compute(pe_profit_loss)
            self._pe_strategy_analysis.compute_equity_curve(pe_profit_loss)
        else:
            if soft_exit:
                pe_exit_type = ExitType.NO_TRADE
                # For safe side, when PE instrument is None (no entry taken) and soft_exit is True
                # make self._trade_pe_next_day to ensure no soft entry taken next day
                self._trade_pe_next_day = False
            else:
                # Code should never reach this condition. This is an invalid exit.
                pe_exit_type = ExitType.INVALID_EXIT

        # Add to output dataframe
        df_data = {
            "Trade": self._trade_count,
            "Script": self.config["script"],
            "Expiry": expiry,
            "LotSize": lot_size,
            "PEStrike": self.PE_ENTRY_INSTRUMENT.strike if self.PE_ENTRY_INSTRUMENT is not None
            else None,
            "PEEntryExitTime": pe_actual_exit_datetime,
            "PESell": pe_exit_price,
            "PEProfitLoss": pe_profit_loss,
            "PEEntryExitType": pe_exit_type,
            "CEStrike": self.CE_ENTRY_INSTRUMENT.strike if self.CE_ENTRY_INSTRUMENT is not None
            else None,
            "CEEntryExitTime": ce_actual_exit_datetime,
            "CEBuy": ce_exit_price,
            "CEProfitLoss": ce_profit_loss,
            "CEEntryExitType": ce_exit_type
        }
        df = pd.DataFrame([list(df_data.values())], columns=list(df_data.keys()))
        self._output_df = pd.concat([self._output_df, df], axis=0)
        # Check if all the lots are exit, make the instrument as None so as to take next
        # entry signal
        if self.PE_ENTRY_INSTRUMENT is not None and self.PE_ENTRY_INSTRUMENT.lot_size == 0:
            self.PE_ENTRY_INSTRUMENT = None
        if self.CE_ENTRY_INSTRUMENT is not None and self.CE_ENTRY_INSTRUMENT.lot_size == 0:
            self.CE_ENTRY_INSTRUMENT = None

    def soft_entry_exit(
            self,
            exit_datetime: datetime.datetime,
            lot_size: int,
            use_pe_atm_strike: bool,
            use_ce_atm_strike: bool,
    ) -> None:
        """
        Logic for soft entry and exit. Used when we close position at day end and take the same
        position again next day.
        exit_datetime is the signal exit datetime
        use_pe_atm_strike: Takes a new entry with atm strike if True else take entry with
        the previous day strike
        """
        final_exit_date = min(exit_datetime.date(), self._active_instrument_expiry)
        exit_date = self._entry_datetime.date()
        while exit_date <= final_exit_date:
            day_exit_datetime = datetime.datetime.combine(
                exit_date, datetime.time(hour=15, minute=25)
            )
            if exit_date == final_exit_date:
                # This is the final day when we should square off the both CE and PE position
                # We are passing signal exit datetime and soft_exit is by default false.
                # So exit status will be calculated in the exit function by comparing signal
                # exit date and expiry
                self.exit(exit_datetime=exit_datetime, lot_size=lot_size)
                break
            self._logger.info(f"Soft exiting at {day_exit_datetime}")
            self.exit(
                exit_datetime=day_exit_datetime,
                lot_size=lot_size,
                soft_exit=True
            )
            if not self._trade_pe_next_day and not self._trade_ce_next_day:
                # This will be True if both CE and PE SL hit before the exit date.
                # If SL mode is deactivated, code shouldn't reach this condition
                break
            exit_date = self.get_next_valid_date(exit_date)
            day_start_datetime = datetime.datetime.combine(
                exit_date, datetime.time(hour=9, minute=15)
            )
            self._logger.info(f"Soft entry at {day_start_datetime}")
            ce_entry_strike = self._entry_strike
            pe_entry_strike = self._entry_strike
            if use_ce_atm_strike or use_pe_atm_strike:
                # Get the nifty atm strike for the next day
                index = self.get_nifty_day_open(day_start_datetime.date())
            if use_ce_atm_strike:
                # If use_ce_atm_strike is True, take the ATM strike as ce_entry_strike for next day
                ce_entry_strike = self._entry_strike if index is None \
                    else self.get_nearest_50_strike(int(index))
            if use_pe_atm_strike:
                # If use_pe_atm_strike is True, take the ATM strike as pe_entry_strike for next day
                pe_entry_strike = self._entry_strike if index is None \
                    else self.get_nearest_50_strike(int(index))
            self.entry(
                ce_entry_strike=ce_entry_strike,
                pe_entry_strike=pe_entry_strike,
                entry_datetime=day_start_datetime,
                lot_size=lot_size,
                expiry=self._active_instrument_expiry,
                soft_entry=True
            )
            # IF there is no entry (neither in CE not in PE), break
            # Check a note for this condition in entry() function. Normally entry() function is
            # called when we need to take an entry for either CE or PE (anyone of the instrument).
            if self.CE_ENTRY_INSTRUMENT is None and self.PE_ENTRY_INSTRUMENT is None:
                break

    def execute(self) -> None:
        """ Execute backtesting """
        start_time = time.time()
        super(HullMABackTesting, self).execute()
        self._ce_strategy_analysis.initial_capital = self.config.get("initial_capital_ce")
        self._pe_strategy_analysis.initial_capital = self.config.get("initial_capital_pe")
        self.process_input()
        self.save_df_to_excel(self._output_df, self.config["output_excel_file_path"])
        self._logger.info(f"Output excel is saved to {self.config['output_excel_file_path']}")
        execution_time = round(time.time() - start_time, 2)
        self._logger.info(f"Execution time: {execution_time} seconds")
        # Print strategy analysis
        print("CE Buy Analysis")
        self._ce_strategy_analysis.print_analysis()
        print("PE Sell Analysis")
        self._pe_strategy_analysis.print_analysis()

    @property
    def ce_strategy_analysis(self) -> StrategyAnalysis:
        return self._ce_strategy_analysis

    @property
    def pe_strategy_analysis(self) -> StrategyAnalysis:
        return self._pe_strategy_analysis
