
import pandas as pd
import numpy as np
import math
import datetime
from fudstop.apis.polygonio.mapping import option_condition_dict
class OptionSnapshotData:
    def __init__(self, data):
        self.implied_volatility = [float(i['implied_volatility']) if 'implied_volatility' in i else None for i in data]
        self.open_interest = [float(i['open_interest']) if 'open_interest' in i else None for i in data]
        self.break_even_price = [float(i['break_even_price']) if 'break_even_price' in i else None for i in data]

        day = [i['day'] if i['day'] is not None else None for i in data]
        self.day_close = [float(i['close']) if 'close' in i else None for i in day]
        self.day_high = [float(i['high']) if 'high' in i else None for i in day]
        self.last_updated  = [i['last_updated'] if 'last_updated' in i else None for i in day]
        self.day_low  = [float(i['low']) if 'low' in i else None for i in day]
        self.day_open  = [float(i['open']) if 'open' in i else None for i in day]
        self.day_change_percent  = [float(i['change_percent']) if 'change_percent' in i else None for i in day]
        self.day_change  = [float(i['change']) if 'change' in i else None for i in day]
        self.previous_close = [float(i['previous_close']) if 'previous_close' in i else None for i in day]
        self.day_volume = [float(i['volume']) if 'volume' in i else None for i in day]
        self.day_vwap  = [float(i['vwap']) if 'vwap' in i else None for i in day]

        details = [i.get('details', None) for i in data]
        self.contract_type = [i['contract_type'] if 'contract_type' in i else None for i in details]
        self.exercise_style = [i['exercise_style'] if 'exercise_style' in i else None for i in details]
        self.expiration_date = [i['expiration_date'] if 'expiration_date' in i else None for i in details]
        self.shares_per_contract= [i['shares_per_contract'] if 'shares_per_contract' in i else None for i in details]
        self.strike_price = [float(i['strike_price']) if 'strike_price' in i else None for i in details]
        self.option_symbol = [i['ticker'] if 'ticker' in i else None for i in details]

        greeks = [i.get('greeks', None) for i in data]
        self.delta = [float(i['delta']) if 'delta' in i else None for i in greeks]
        self.gamma= [float(i['gamma']) if 'gamma' in i else None for i in greeks]
        self.theta= [float(i['theta']) if 'theta' in i else None for i in greeks]
        self.vega = [float(i['vega']) if 'vega' in i else None for i in greeks]

        lastquote = [i.get('last_quote',None) for i in data]
        self.ask = [float(i['ask']) if 'ask' in i else None for i in lastquote]
        self.ask_size = [float(i['ask_size']) if 'ask_size' in i else None for i in lastquote]
        self.bid= [float(i['bid']) if 'bid' in i else None for i in lastquote]
        self.bid_size= [float(i['bid_size']) if 'bid_size' in i else None for i in lastquote]
        self.quote_last_updated= [i['quote_last_updated'] if 'quote_last_updated' in i else None for i in lastquote]
        self.midpoint = [float(i['midpoint']) if 'midpoint' in i else None for i in lastquote]


        lasttrade = [i['last_trade'] if i['last_trade'] is not None else None for i in data]
        self.conditions = [i['conditions'] if 'conditions' in i else None for i in lasttrade]
        self.exchange = [i['exchange'] if 'exchange' in i else None for i in lasttrade]
        self.price= [float(i['price']) if 'price' in i else None for i in lasttrade]
        self.sip_timestamp= [i['sip_timestamp'] if 'sip_timestamp' in i else None for i in lasttrade]
        self.size= [float(['size']) if 'size' in i else None for i in lasttrade]

        underlying = [i['underlying_asset'] if i['underlying_asset'] is not None else None for i in data]
        self.change_to_break_even = [i['change_to_break_even'] if 'change_to_break_even' in i else None for i in underlying]
        self.underlying_last_updated = [i['underlying_last_updated'] if 'underlying_last_updated' in i else None for i in underlying]
        self.underlying_price = [float(i['price']) if 'price' in i else None for i in underlying]
        self.underlying_ticker = [i['ticker'] if 'ticker' in i else None for i in underlying]


 # Calculate time to maturity for each option
        self.time_to_maturity = [
            self.years_to_maturity(exp_date) for exp_date in self.expiration_date
        ]

        self.data_dict = {
        "iv": self.implied_volatility,
        "oi": self.open_interest,
        "break_even_price": self.break_even_price,
        "close": self.day_close,
        "high": self.day_high,
        "last_updated": self.last_updated,
        "low": self.day_low,
        "open": self.day_open,
        "change_percent": self.day_change_percent,
        "change": self.day_change,
        "previous_close": self.previous_close,
        "vol": self.day_volume,
        "vwap": self.day_vwap,
        "call_put": self.contract_type,
        "exercise_style": self.exercise_style,
        "exp": self.expiration_date,
        "shares_per_contract": self.shares_per_contract,
        "strike": self.strike_price,
        "ticker": self.option_symbol,

        "delta": self.delta,
        "gamma": self.gamma,
        "theta": self.theta,
        "vega": self.vega,
        "ask": self.ask,
        "ask_size": self.ask_size,
        "bid": self.bid,
        "bid_size": self.bid_size,
        "quote_last_updated": self.quote_last_updated,
        "midpoint": self.midpoint,
        "conditions": self.conditions,
        "exchange": self.exchange,
        "cost": self.price,
        "timestamp": self.sip_timestamp,
        "size": self.size,
        "change_to_break_even": self.change_to_break_even,
        "underlying_last_updated": self.underlying_last_updated,
        "price": self.underlying_price,
        "symbol": self.underlying_ticker
    }


        self.df = pd.DataFrame(self.data_dict)





class WorkingUniversal:
    def __init__(self, data):
        self.risk_free_rate = 4.25
        
        # We'll gather all rows here
        rows = []

        for item in data:
            # Begin building a dictionary for this row
            row_dict = {}
            
            # Extract top-level fields
            row_dict['break_even_price'] = item.get('break_even_price')
            row_dict['name'] = item.get('name')
            row_dict['market_status'] = item.get('market_status')
            row_dict['option_symbol'] = item.get('ticker')
            row_dict['type'] = item.get('type')
            
            # 1) Session-related fields
            session = item.get('session', {})
            row_dict['change'] = session.get('change')
            row_dict['change_percent'] = session.get('change_percent')
            row_dict['close'] = session.get('close')
            row_dict['high'] = session.get('high')
            row_dict['low'] = session.get('low')
            row_dict['open'] = session.get('open')
            row_dict['volume'] = session.get('volume')
            row_dict['previous_close'] = session.get('previous_close')

            # 2) Details fields
            details = item.get('details', {})
            row_dict['call_put'] = details.get('contract_type')
            row_dict['exercise_style'] = details.get('exercise_style')
            row_dict['expiry'] = details.get('expiration_date')
            row_dict['shares_per_contract'] = details.get('shares_per_contract')
            row_dict['strike'] = details.get('strike_price')

            # 3) Greeks
            greeks = item.get('greeks', {})
            row_dict['delta'] = greeks.get('delta')
            row_dict['gamma'] = greeks.get('gamma')
            row_dict['theta'] = greeks.get('theta')
            row_dict['vega'] = greeks.get('vega')

            # 4) Implied Volatility
            row_dict['iv'] = item.get('implied_volatility')

            # 5) Last Quote
            last_quote = item.get('last_quote', {})
            row_dict['ask'] = last_quote.get('ask')
            row_dict['ask_size'] = last_quote.get('ask_size')
            row_dict['ask_exchange'] = last_quote.get('ask_exchange')
            row_dict['bid'] = last_quote.get('bid')
            row_dict['bid_size'] = last_quote.get('bid_size')
            row_dict['bid_exchange'] = last_quote.get('bid_exchange')
            row_dict['midpoint'] = last_quote.get('midpoint')

            # 6) Last Trade
            last_trade = item.get('last_trade', {})
            row_dict['sip_timestamp'] = last_trade.get('sip_timestamp')
            # conditions might be a list; if so, join them
            conditions = last_trade.get('conditions', [])
            conditions_str = ','.join(map(str, conditions))
            # example usage: convert the first condition to an int, then use option_condition_dict
            if conditions_str:
                try:
                    int_first = int(conditions_str.split(',')[0])
                    row_dict['trade_conditions'] = option_condition_dict.get(int_first)
                except:
                    row_dict['trade_conditions'] = None
            else:
                row_dict['trade_conditions'] = None

            row_dict['trade_price'] = last_trade.get('price')
            row_dict['trade_size'] = last_trade.get('size')
            row_dict['trade_exchange'] = last_trade.get('exchange')

            # 7) Open Interest
            row_dict['oi'] = item.get('open_interest')

            # 8) Underlying Asset
            underlying_asset = item.get('underlying_asset', {})
            row_dict['change_to_break_even'] = underlying_asset.get('change_to_break_even')
            row_dict['underlying_price'] = underlying_asset.get('price')
            row_dict['ticker'] = underlying_asset.get('ticker')  # e.g. underlying_ticker

            # Done collecting for this single row
            rows.append(row_dict)

        # Build initial DataFrame
        self.as_dataframe = pd.DataFrame(rows)
        
        # Now all columns have length == len(data).
        
        # Additional computations
        self.as_dataframe['dte'] = self.as_dataframe['expiry'].apply(self.compute_dte)

        self.as_dataframe['risk_free_rate'] = self.risk_free_rate
        
        # Intrinsic/Extrinsic Value
        self.as_dataframe['intrinsic_value'] = self.as_dataframe.apply(
            lambda row: self.compute_intrinsic_value(
                row['underlying_price'],
                row['strike'],
                row['call_put']
            ), axis=1
        )
        self.as_dataframe['extrinsic_value'] = self.as_dataframe.apply(
            lambda row: self.compute_extrinsic_value(
                row['midpoint'],
                row['intrinsic_value']
            ), axis=1
        )

        # Spread and spread %
        self.as_dataframe['spread'] = self.as_dataframe.apply(
            lambda row: (row['ask'] - row['bid']) if row['ask'] and row['bid'] else None,
            axis=1
        )
        self.as_dataframe['spread_pct'] = self.as_dataframe.apply(
            lambda row: ((row['ask'] - row['bid']) / row['midpoint'] * 100)
                        if row['ask'] and row['bid'] and row['midpoint'] else None,
            axis=1
        )

        # Premium %
        self.as_dataframe['premium_percent'] = self.as_dataframe.apply(
            lambda row: (row['midpoint'] / row['underlying_price'] * 100)
                        if row['midpoint'] and row['underlying_price'] else None,
            axis=1
        )

        # volume / oi ratio
        self.as_dataframe['vol_oi_ratio'] = self.as_dataframe.apply(
            lambda row: row['volume'] / row['oi'] if row['volume'] and row['oi'] else None,
            axis=1
        )

        # Moneyness
        self.as_dataframe['moneyness'] = self.as_dataframe.apply(self.compute_moneyness, axis=1)

        # # Additional Greeks
        # self.add_additional_greeks()
        
        # Calculate IV skew metrics if needed
        self.calc_iv_skew()

    @staticmethod
    def compute_dte(expiry_str):
        """
        Days to expiration (DTE) from expiry_str, which is expected to be YYYY-MM-DD or similar ISO format.
        """
        if not expiry_str:
            return None
        try:
            exp_date = datetime.datetime.fromisoformat(expiry_str)
            return max((exp_date - datetime.datetime.now()).days, 0)
        except:
            return None

    @staticmethod
    def compute_intrinsic_value(S, K, call_put):
        """
        Intrinsic value for a single option:
          call = max(S - K, 0)
          put = max(K - S, 0)
        """
        if S is None or K is None or call_put is None:
            return None
        if call_put.lower() == 'call':
            return max(S - K, 0)
        elif call_put.lower() == 'put':
            return max(K - S, 0)
        else:
            return None

    @staticmethod
    def compute_extrinsic_value(midpoint, intrinsic):
        """
        Extrinsic = max(midpoint - intrinsic, 0)
        """
        if midpoint is None or intrinsic is None:
            return None
        return max(midpoint - intrinsic, 0)

    @staticmethod
    def compute_moneyness(row):
        """
        Compute whether the option is atm, itm, or otm based on call/put, strike, and underlying price.
        """
        call_put = row['call_put']
        S = row['underlying_price']
        K = row['strike']
        if not call_put or not S or not K:
            return None
        call_put = call_put.lower()

        if call_put == 'call':
            if S > K:
                return 'itm'
            elif S < K:
                return 'otm'
            else:
                return 'atm'
        elif call_put == 'put':
            if S < K:
                return 'itm'
            elif S > K:
                return 'otm'
            else:
                return 'atm'
        return None

    # def add_additional_greeks(self):
    #     """
    #     Compute advanced greeks row-by-row and attach them to DataFrame columns.
    #     """
    #     advanced = []
    #     for idx, row in self.as_dataframe.iterrows():
    #         S   = row['underlying_price']
    #         T   = row['time_to_maturity']
    #         vol = row['iv']
    #         gam = row['gamma']
    #         adv = self.compute_additional_greeks(
    #             S, T, vol, gam, r=self.risk_free_rate/100.0
    #         )
    #         advanced.append(adv)

    #     for greek in ['vanna','vomma','veta','vera','speed','zomma','color','ultima','charm']:
    #         self.as_dataframe[greek] = [adv[greek] for adv in advanced]

    @staticmethod
    def compute_additional_greeks(S, T, sigma, gamma, *, r=0.0):
        """
        Compute advanced Greeks in a safe way. Returns a dict with all fields or None if not computable.
        """
        result = {
            'vanna': None, 'vomma': None, 'veta': None, 'vera': None,
            'speed': None, 'zomma': None, 'color': None, 'ultima': None,
            'charm': None
        }
        if not (S and T and sigma and gamma):
            return result
        try:
            # simplistic examples
            result['vanna'] = gamma / sigma
            result['vomma'] = gamma / sigma**2
            result['veta']  = -S * sigma * gamma * T
            result['vera']  = gamma / r if r != 0 else None
            result['speed'] = -gamma / S
            result['zomma'] = 2 * gamma
            result['color'] = -gamma / T
            result['ultima'] = gamma / sigma**3
            result['charm'] = -gamma * 2 / T  
        except:
            pass
        return result

    def calc_iv_skew(self):
        """
        Calculate IV skew by searching for the lowest implied volatility strike price, etc.
        Example logic that sets 'iv_skew' column to 'call_skew' or 'put_skew'.
        """
        df = self.as_dataframe
        # filter out rows that have valid strike & iv
        valid = df.dropna(subset=['strike','iv','underlying_price'])
        if valid.empty:
            self.as_dataframe['iv_skew'] = None
            self.as_dataframe['avg_iv'] = None
            return
        
        # average implied volatility
        avg_iv = valid['iv'].mean()
        self.as_dataframe['avg_iv'] = avg_iv
        
        # find row with lowest iv
        idxmin = valid['iv'].idxmin()
        lowest_iv_strike = valid.loc[idxmin, 'strike']
        underlying_price = valid.loc[idxmin, 'underlying_price']
        
        def which_skew(row):
            # If the row's strike is above the underlying price, we might say 'call_skew'; otherwise 'put_skew'
            # (You can define your own logic)
            if pd.isna(row['strike']) or pd.isna(row['underlying_price']):
                return None
            return 'call_skew' if row['strike'] > row['underlying_price'] else 'put_skew'

        # For simplicity, set iv_skew for **all** rows to the skew based on the lowest-iv strike row
        skew_label = 'call_skew' if lowest_iv_strike > underlying_price else 'put_skew'
        self.as_dataframe['iv_skew'] = skew_label