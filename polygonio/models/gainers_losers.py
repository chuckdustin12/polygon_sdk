#!/usr/bin/env python3
"""
Module: gainers_losers.py
-------------------------
This module defines the GainersLosers class to process and structure stock snapshot data
(for gainers/losers) from the Polygon.io API. It extracts various fields (e.g. ticker,
day-level market data, last quote/trade data, minute-level and previous day data), converts
timestamps to human-readable strings, and aggregates everything into a Pandas DataFrame.

Usage:
    >>> from gainers_losers import GainersLosers
    >>> data = [ ... ]  # List of dictionaries from the API
    >>> gl = GainersLosers(data)
    >>> df = gl.as_dataframe
"""

import pandas as pd
from datetime import datetime


class GainersLosers:
    """
    Processes gainers/losers data from an API response and provides a structured Pandas DataFrame.

    The class expects a list of dictionaries where each dictionary contains keys such as 'ticker',
    'day', 'lastQuote', 'lastTrade', 'min', and 'prevDay'. Timestamps are converted to a human-readable
    format. The resulting DataFrame is available via the `as_dataframe` attribute.
    """

    def __init__(self, data: list[dict]) -> None:
        # Top-level fields
        self.ticker = [item.get('ticker') for item in data]
        self.todaysChangePerc = [float(item.get('todaysChangePerc')) for item in data]
        self.todaysChange = [float(item.get('todaysChange')) for item in data]
        self.updated = [item.get('updated') for item in data]

        # Extract day-level data from the "day" key
        day = [item.get('day') for item in data]
        self.day_open = [float(d.get('o')) if d and d.get('o') is not None else None for d in day]
        self.day_high = [float(d.get('h')) if d and d.get('h') is not None else None for d in day]
        self.day_low = [float(d.get('l')) if d and d.get('l') is not None else None for d in day]
        self.day_close = [float(d.get('c')) if d and d.get('c') is not None else None for d in day]
        self.day_vol = [float(d.get('v')) if d and d.get('v') is not None else None for d in day]
        self.day_vwap = [float(d.get('vw')) if d and d.get('vw') is not None else None for d in day]

        # Extract last quote data from the "lastQuote" key
        last_quote = [item.get('lastQuote') for item in data]
        self.ask = [float(lq.get('P')) if lq and lq.get('P') is not None else None for lq in last_quote]
        self.ask_size = [float(lq.get('S')) if lq and lq.get('S') is not None else None for lq in last_quote]
        self.bid = [float(lq.get('p')) if lq and lq.get('p') is not None else None for lq in last_quote]
        self.bid_size = [float(lq.get('s')) if lq and lq.get('s') is not None else None for lq in last_quote]
        # Convert timestamps (assumed to be in nanoseconds) to a human-readable format
        self.quote_timestamp = [
            datetime.fromtimestamp(lq.get('t') / 1e9).strftime('%Y-%m-%d %H:%M:%S')
            if lq and lq.get('t') is not None else None for lq in last_quote
        ]

        # Extract last trade data from the "lastTrade" key
        last_trade = [item.get('lastTrade') for item in data]
        self.conditions = [lt.get('c') if lt else None for lt in last_trade]
        self.trade_id = [lt.get('i') if lt else None for lt in last_trade]
        self.trade_price = [
            float(lt.get('p')) if lt and lt.get('p') is not None else None for lt in last_trade
        ]
        self.trade_size = [
            float(lt.get('s')) if lt and lt.get('s') is not None else None for lt in last_trade
        ]
        self.trade_timestamp = [
            datetime.fromtimestamp(lt.get('t') / 1e9).strftime('%Y-%m-%d %H:%M:%S')
            if lt and lt.get('t') is not None else None for lt in last_trade
        ]
        self.trade_exchange = [lt.get('x') if lt else None for lt in last_trade]

        # Extract minute-level data from the "min" key
        min_data = [item.get('min') for item in data]
        # Overwriting self.day_vol with minute-level average volume.
        # If this is not intended, consider renaming this attribute.
        self.day_vol = [float(m.get('av')) if m and m.get('av') is not None else None for m in min_data]
        self.min_vol = [float(m.get('v')) if m and m.get('v') is not None else None for m in min_data]
        self.min_vwap = [float(m.get('vw')) if m and m.get('vw') is not None else None for m in min_data]
        self.min_open = [float(m.get('o')) if m and m.get('o') is not None else None for m in min_data]
        self.min_high = [float(m.get('h')) if m and m.get('h') is not None else None for m in min_data]
        self.min_low = [float(m.get('l')) if m and m.get('l') is not None else None for m in min_data]
        self.min_close = [float(m.get('c')) if m and m.get('c') is not None else None for m in min_data]
        self.min_trades = [float(m.get('n')) if m and m.get('n') is not None else None for m in min_data]
        self.min_timestamp = [
            datetime.fromtimestamp(m.get('t') / 1000).strftime('%Y-%m-%d %H:%M:%S')
            if m and m.get('t') is not None else None for m in min_data
        ]

        # Extract previous day data from the "prevDay" key
        prev_day = [item.get('prevDay') for item in data]
        self.prev_open = [float(pd_item.get('o')) if pd_item and pd_item.get('o') is not None else None for pd_item in prev_day]
        self.prev_high = [float(pd_item.get('h')) if pd_item and pd_item.get('h') is not None else None for pd_item in prev_day]
        self.prev_low = [float(pd_item.get('l')) if pd_item and pd_item.get('l') is not None else None for pd_item in prev_day]
        self.prev_close = [float(pd_item.get('c')) if pd_item and pd_item.get('c') is not None else None for pd_item in prev_day]
        self.prev_vol = [float(pd_item.get('v')) if pd_item and pd_item.get('v') is not None else None for pd_item in prev_day]
        self.prev_vwap = [float(pd_item.get('vw')) if pd_item and pd_item.get('vw') is not None else None for pd_item in prev_day]

        # Build data dictionary for DataFrame construction
        self.data_dict = { 
            'ticker': self.ticker,
            'day_open': self.day_open,
            'day_high': self.day_high,
            'day_low': self.day_low,
            'day_close': self.day_close,
            'day_vol': self.day_vol,
            'day_vwap': self.day_vwap,
            'day_change': self.todaysChange,
            'day_changeperc': self.todaysChangePerc,
            'prev_open': self.prev_open,
            'prev_high': self.prev_high,
            'prev_low': self.prev_low,
            'prev_close': self.prev_close,
            'prev_vol': self.prev_vol,
            'prev_vwap': self.prev_vwap,
            'min_open': self.min_open,
            'min_high': self.min_high,
            'min_low': self.min_low,
            'min_close': self.min_close,
            'min_volume': self.min_vol,
            'min_vwap': self.min_vwap,
            'min_trades': self.min_trades,
            'min_time': self.min_timestamp,
            'trade_id': self.trade_id,
            'trade_conditions': self.conditions,
            'trade_exchange': self.trade_exchange,
            'trade_size': self.trade_size,
            'trade_price': self.trade_price,
            'trade_time': self.trade_timestamp,
            'ask': self.ask,
            'ask_size': self.ask_size,
            'bid': self.bid,
            'bid_size': self.bid_size,
            'quote_time': self.quote_timestamp
        }

        self.as_dataframe = pd.DataFrame(self.data_dict)


# if __name__ == '__main__':
#     # Sample data for testing the GainersLosers class
#     sample_data = [
#         {
#             "ticker": "AAPL",
#             "todaysChangePerc": "1.25",
#             "todaysChange": "2.50",
#             "updated": "2024-01-01T10:00:00Z",
#             "day": {"o": "150", "h": "155", "l": "149", "c": "154", "v": "1000000", "vw": "152"},
#             "lastQuote": {"P": "154.5", "S": "100", "p": "154.4", "s": "200", "t": 1700000000000000000},
#             "lastTrade": {"c": ["cond1", "cond2"], "i": "trade123", "p": "154.4", "s": "50", "t": 1700000000000000000, "x": "NASDAQ"},
#             "min": {"av": "950000", "v": "50000", "vw": "152", "o": "150", "h": "155", "l": "149", "c": "154", "n": "120", "t": 1700000000000},
#             "prevDay": {"o": "148", "h": "152", "l": "147", "c": "150", "v": "900000", "vw": "149"}
#         }
#     ]
#     gl = GainersLosers(sample_data)
#     print("GainersLosers DataFrame:")
#     print(gl.as_dataframe)
