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

from src.backtesting.base_backtesting import BaseBackTesting
from src.backtesting.constant import SignalType, ExitType, EntryType
from src.backtesting.instrument import Instrument
from src.backtesting.historical_data.db_api import DBApi
from src.backtesting.exception import BackTestingError
from src.utils.logger import LogFacade


class HullMABackTesting(BaseBackTesting):
    """ Hull moving average backtesting """
    OUTPUT_EXCEL_COLUMNS = [
        "Trade", "Script", "Strike", "Expiry", "LotSize", "PEEntryExitTime", "PESell",
        "PEProfitLoss", "PEEntryExitType", "CEEntryExitTime", "CEBuy", "CEProfitLoss",
        "CEEntryExitType"
    ]

    def __init__(self, config_file_path: str):
        super(HullMABackTesting, self).__init__(config_file_path=config_file_path)
        self._ce_historical_data: List[sqlite3.Row] = []
        self._pe_historical_data: List[sqlite3.Row] = []
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

    def process_input(self):
        """ Process the input excel row by row """
        self._logger.info(
            f"Reading and processing input excel file {self.config['input_excel_file_path']}"
        )
        input_df = self.read_input_excel_to_df(Path(self.config["input_excel_file_path"]))
        close_position_day_end = self.config.get("close_position_day_end", False)
        for index, row in input_df.iterrows():
            # Signal type is entry and no entry has taken
            if row["Signal"] == SignalType.ENTRY and not self.is_entry_taken():
                self._logger.info(
                    f"Entry signal triggered at {row['Date/Time']} for price {row['Price']}"
                )
                self._entry_datetime = self.get_market_hour_datetime(row["Date/Time"])
                self._entry_strike = self.get_entry_strike(row["Price"])
                self._active_instrument_expiry = self.get_current_week_expiry(
                    self._entry_datetime.date()
                )
                lot_size = int(row["Contracts"])
                self.entry(
                    entry_strike=self._entry_strike,
                    entry_datetime=self._entry_datetime,
                    lot_size=lot_size,
                    expiry=self._active_instrument_expiry
                )
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
                    self.soft_entry_exit(exit_datetime=exit_datetime, lot_size=lot_size)
                else:
                    self.exit(exit_datetime=exit_datetime, lot_size=lot_size)

    def fetch_historical_price(self, index: str, strike: int, expiry: datetime.date) -> None:
        """ Get the list of CE historical price data """
        self._logger.info(f"Fetching historical data for {index} {strike} for expiry {expiry}")
        with DBApi(Path(self.config["db_file_path"])) as db_api:
            self._ce_historical_data = db_api.fetch_historical_data(
                index=index,
                strike=strike,
                option_type="CE",
                expiry=expiry
            )
            self._pe_historical_data = db_api.fetch_historical_data(
                index=index,
                strike=strike,
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
                if datetime.datetime.strptime(x["dt"], "%Y-%m-%d %H:%M:%S.%f").date() == dt.date()
                   and datetime.datetime.strptime(x["dt"], "%Y-%m-%d %H:%M:%S.%f") >= dt
            ),
            None
        )
        if price_data is None:
            raise BackTestingError(f"price data is missing for {dt}")
        return float(price_data["close"])

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
    ) -> List[sqlite3.Row]:
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
            if start_datetime < datetime.datetime.strptime(x["dt"], "%Y-%m-%d %H:%M:%S.%f") <
               end_datetime
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
        if instrument.option_type == "CE":
            # Its a long trade. So Stop Loss will be below the long price
            instrument_sl_price = round((100 - stop_loss) * instrument.price / 100, 2)
        else:
            # Its a short trade. So Stop Loss will be above the short price
            instrument_sl_price = round((100 + stop_loss) * instrument.price / 100, 2)
        for data in price_data:
            # Long trade. If price goes below instrument_sl_price, SL hit
            if instrument.option_type == "CE" and float(data["close"]) < instrument_sl_price:
                return True, float(data["close"]), \
                       datetime.datetime.strptime(data["dt"], "%Y-%m-%d %H:%M:%S.%f")
            # Short trade. If price goes above instrument_sl_price, SL hit
            elif instrument.option_type == "PE" and float(data["close"]) > instrument_sl_price:
                return True, float(data["close"]), \
                       datetime.datetime.strptime(data["dt"], "%Y-%m-%d %H:%M:%S.%f")
        return False, None, None

    def entry(
            self,
            entry_strike: int,
            entry_datetime: datetime.datetime,
            lot_size: int,
            expiry: datetime.date,
            soft_entry: bool = False
    ) -> None:
        """
        Logic for trade entry. soft_entry is used when we close the position at dat end and open
        the same position again next day.
        """

        # Get historical data for both PE and CE for entry strike and expiry
        self.fetch_historical_price(
            index=self.config["script"], strike=entry_strike, expiry=expiry
        )
        # Take CE entry when soft_entry is False (actual signal entry) or when soft_entry is True
        # and self._trade_ce_next_day is True which means SL not hit on previous day
        # Soft entry is true when we close an entry on the day end and take the same entry next day
        ce_instrument_price = 0
        ce_entry_type = ""
        if not soft_entry or (soft_entry and self._trade_ce_next_day):
            self.CE_ENTRY_INSTRUMENT = self.get_ce_entry_instrument(
                entry_strike=entry_strike,
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
        # Take PE entry when soft_entry is False (actual signal entry) or when soft_entry is True
        # and self._trade_ce_next_day is True which means SL not hit on previous day
        # Soft entry is true when we close an entry on the day end and take the same entry next day
        pe_instrument_price = 0
        pe_entry_type = ""
        if not soft_entry or (soft_entry and self._trade_pe_next_day):
            self.PE_ENTRY_INSTRUMENT = self.get_pe_entry_instrument(
                entry_strike=entry_strike,
                entry_datetime=entry_datetime,
                lot_size=lot_size,
                expiry=expiry
            )
            pe_entry_type = EntryType.SOFT_ENTRY if soft_entry else EntryType.ENTRY_SIGNAL
            pe_instrument_price = self.PE_ENTRY_INSTRUMENT.price
        self._trade_count += 1
        df_data = {
            "Trade": self._trade_count,
            "Script": self.config["script"],
            "Strike": entry_strike,
            "Expiry": expiry,
            "LotSize": self.PE_ENTRY_INSTRUMENT.lot_size if self.PE_ENTRY_INSTRUMENT is not None
            else self.CE_ENTRY_INSTRUMENT.lot_size,
            "PEEntryExitTime": entry_datetime,
            "PESell": pe_instrument_price,
            "PEProfitLoss": 0,
            "PEEntryExitType": pe_entry_type,
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
        pe_instrument_sl_hit = False
        sl_check = self.config.get("SL_check")      # Get SL_check from config file
        # Get CE exit price
        ce_exit_price = 0
        ce_profit_loss = 0
        if self.CE_ENTRY_INSTRUMENT is not None:
            # Check if SL exit mode is set in config file. If yes, check minute by minute data
            # to see if SL is getting hit
            if sl_check is not None:
                ce_instrument_sl_hit, ce_exit_price, sl_datetime = self.instrument_stop_loss_hit(
                    instrument=self.CE_ENTRY_INSTRUMENT,
                    stop_loss=sl_check["CE"],
                    exit_datetime=ce_actual_exit_datetime
                )
            if ce_instrument_sl_hit:
                # This is True when SL check is ON and CE SL hits
                ce_actual_exit_datetime = sl_datetime
                ce_exit_type = ExitType.SL_EXIT
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
        else:
            if soft_exit:
                ce_exit_type = ExitType.NO_TRADE
            else:
                ce_exit_type = ExitType.CE_PREMIUM_EXIT

        # Get PE exit price
        pe_exit_price = 0
        pe_profit_loss = 0
        if self.PE_ENTRY_INSTRUMENT is not None:
            # Check if SL exit mode is set in config file. If yes, check minute by minute data
            # to see if SL is getting hit
            if sl_check is not None:
                pe_instrument_sl_hit, pe_exit_price, sl_datetime = self.instrument_stop_loss_hit(
                    instrument=self.PE_ENTRY_INSTRUMENT,
                    stop_loss=sl_check["PE"],
                    exit_datetime=pe_actual_exit_datetime
                )
            if pe_instrument_sl_hit:
                # This is True when SL check is ON and CE SL hits
                pe_actual_exit_datetime = sl_datetime
                pe_exit_type = ExitType.SL_EXIT
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
        else:
            if soft_exit:
                pe_exit_type = ExitType.NO_TRADE
            else:
                # Code should never reach this condition. This is an invalid exit.
                pe_exit_type = ExitType.INVALID_EXIT

        # Add to output dataframe
        df_data = {
            "Trade": self._trade_count,
            "Script": self.config["script"],
            "Strike": active_instrument.strike,
            "Expiry": expiry,
            "LotSize": lot_size,
            "PEEntryExitTime": pe_actual_exit_datetime,
            "PESell": pe_exit_price,
            "PEProfitLoss": pe_profit_loss,
            "PEEntryExitType": pe_exit_type,
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
        # In soft exit mode, if SL not hit then take the same position next day
        if soft_exit:
            if not ce_instrument_sl_hit:
                self._trade_ce_next_day = True
            if not pe_instrument_sl_hit:
                self._trade_pe_next_day = True

    def soft_entry_exit(self, exit_datetime: datetime.datetime, lot_size: int) -> None:
        """
        Logic for soft entry and exit. Used when we close position at day end and take the same
        position again next day.
        exit_datetime is the signal exit datetime
        """
        final_exit_date = min(exit_datetime.date(), self._active_instrument_expiry)
        exit_date = self._entry_datetime.date()
        while exit_date <= final_exit_date:
            day_exit_datetime = datetime.datetime.combine(
                exit_date, datetime.time(hour=15, minute=29)
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
            self.entry(
                entry_strike=self._entry_strike,
                entry_datetime=day_start_datetime,
                lot_size=lot_size,
                expiry=self._active_instrument_expiry,
                soft_entry=True
            )

    def execute(self) -> None:
        """ Execute backtesting """
        start_time = time.time()
        super(HullMABackTesting, self).execute()
        self.process_input()
        self.save_df_to_excel(self._output_df, self.config["output_excel_file_path"])
        self._logger.info(f"Output excel is saved to {self.config['output_excel_file_path']}")
        execution_time = time.time() - start_time
        self._logger.info(f"Execution time: {execution_time} seconds")
