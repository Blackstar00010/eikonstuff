import pandas as pd
import eikon as ek
import platform
from Misc.useful_stuff import beep


if platform.system() == 'Darwin':
    print('Your Operating System is Darwin, so eikon Data API was not set.')

else:
    apikey = "123qweasd"
    ek.set_app_key(apikey)

_not_my_fault = [400, 401, 408, 500, 2504]
# 400: Backend error, 401: Eikon Proxy not running, 408: HTTP Timeout
# 500: Backend error, 2504: Gateway Time-out


class Company:
    def __init__(self, comp_code):
        """
        :param comp_code: an RIC code as a str
        """
        self.ric_code = comp_code
        self.price_data = pd.DataFrame()

    def fetch_shrout(self, start: str, end: str) -> pd.DataFrame:
        """
        fetches shares outstanding data
        :param start: the date from which data is fetched given in the format 'YYYY-MM-DD'
        :param end: the data until which data is fetched given in the format 'YYYY-MM-DD'
        :return: pd.DataFrame of columns 'SHROUT' and 'Date'
        """
        try:
            shrout, err = ek.get_data(self.ric_code,
                                      ['TR.SharesOutstanding', 'TR.SharesOutstanding.calcdate'],
                                      {'SDate': start, 'EDate': end}, field_name=True)
            shrout = shrout.drop('Instrument', axis=1)
        except ek.EikonError as eke:
            print('Error code: ', eke.code)
            if eke.code in _not_my_fault:
                # sleep(1)
                shrout = self.fetch_shrout(start=start, end=end)
            elif eke.code == 429:
                beep()
                raise RuntimeError('Reached API calls limit')
            else:
                print(f"{self.ric_code}: No data available for {start} - {end}")
                return pd.DataFrame()
        shrout = shrout.rename(columns={'TR.SHARESOUTSTANDING': 'SHROUT', 'TR.SHARESOUTSTANDING.calcdate': 'Date'})
        return shrout

    def fetch_price_decade(self, dec: int, adj=False) -> pd.DataFrame:
        """
        fetches ohlccv data of a decade and returns as a dataframe.
        :param adj: adjusted price data if True, unadjusted price if False.
        :param dec: start year an integer. (e.g. 2020)
        :return: dataframe of index Date and columns HIGH, LOW, CLOSE, OPEN, COUNT, VOLUME.
                 Also contains SHROUT and MVE columns if `adj` is False.
        """
        corax = 'adjusted' if adj else 'unadjusted'
        try:
            ohlccv = ek.get_timeseries(self.ric_code, start_date=str(dec) + "-01-01", end_date=str(dec + 9) + '-12-31',
                                       corax=corax)
            if (not adj) and (len(ohlccv) > 0):
                shrout = self.fetch_shrout(start=ohlccv.index.min().strftime('%Y-%m-%d'),
                                           end=ohlccv.index.max().strftime('%Y-%m-%d'))
                # shrout has 'Date' column but ohlccv has 'Date' index
                shrout['Date'] = pd.to_datetime(shrout['Date'])
                shrout = shrout.set_index('Date')
                ohlccv = pd.concat([ohlccv, shrout], axis=1).sort_index()
                ohlccv['CLOSE'] = ohlccv['CLOSE'].fillna(method='ffill')
                ohlccv['MVE'] = ohlccv['CLOSE'] * ohlccv['SHROUT']
            return ohlccv
        except ek.EikonError as eke:
            print('Error code: ', eke.code)
            if eke.code in _not_my_fault:
                # 401: Eikon Proxy not running, 500: Backend error, 2504: Gateway Time-out
                # sleep(1)
                return self.fetch_price_decade(dec, adj=adj)
            elif eke.code == 429:
                beep()
                raise RuntimeError('Code 429: reached API calls limit')
            else:
                print(f"{self.ric_code}: No data available for {dec}-{dec + 9}")
            return pd.DataFrame()

    def fetch_price(self, overwrite=False, delisted=2024, adj=False) -> pd.DataFrame:
        """
        fetches ohlccv data and returns as a dataframe.
        :param adj: adjusted price data if True, unadjusted price if False.
        :param delisted: the year the firm got delisted
        :param overwrite: overwrites price data dataframe if it is not empty
        :return: dataframe of index Date and columns HIGH, LOW, CLOSE, OPEN, COUNT, VOLUME.
                 Also contains SHROUT and MVE columns if `adj` is False.
        """

        if overwrite or self.price_data.empty:
            startyear = 1983
            df = self.fetch_price_decade(startyear, adj=adj)
            startyear += 10
            while startyear < delisted:
                new_df = self.fetch_price_decade(startyear, adj=adj)
                df = pd.concat([df, new_df], axis=0)
                startyear += 10
            self.price_data = df
        return self.price_data


class Companies:
    def __init__(self, code_list: list):
        """
        :param code_list: list of RIC codes
        """
        self.ric_codes = code_list
        self.isins = []
        self.cusips = []
        self.sedols = []
        self._data_list = []
        self._raw_data_list = []

    def fetch_symb(self, symb_type: str) -> pd.DataFrame:
        """
        Returns translation dataframe of ISIN/CUSIP/SEDOL.
        :param symb_type: 'ISIN', 'CUSIP' or 'SEDOL'
        :return: pd.DataFrame of columns 'RIC' and 'ISIN'/'CUSIP'/'SEDOL'
        """
        result = ek.get_symbology(self.ric_codes, from_symbol_type='RIC', to_symbol_type=symb_type)
        result = result.reset_index()
        result = result.rename(columns={'index': 'RIC'})
        try:
            symb = result[['RIC', symb_type]]
        except KeyError:
            result[symb_type] = ''
            symb = result[['RIC', symb_type]]
        except ek.EikonError as eke:
            print('Error code: ', eke.code)
            symb = self.fetch_symb(symb_type=symb_type)
        return symb

    def fetch_isin(self) -> pd.DataFrame:
        """
        Returns translation dataframe of ISIN Codes. Recommend using `fetch_symb('ISIN')` instead.
        :return: pd.DataFrame of columns `RIC` and `ISIN`
        """
        self.isins = self.fetch_symb('ISIN')
        return self.isins

    def fetch_cusip(self) -> pd.DataFrame:
        """
        Returns translation dataframe of CUSIP Codes. Recommend using `fetch_symb('CUSIP')` instead.
        :return: pd.DataFrame of columns `RIC` and `CUSIP`
        """
        self.cusips = self.fetch_symb('CUSIP')
        return self.cusips

    def fetch_sedol(self) -> pd.DataFrame:
        """
        Returns translation dataframe of SEDOL Codes. Recommend using `fetch_symb('SEDOL')` instead.
        :return: pd.DataFrame of columns `RIC` and `SEDOL`
        """
        self.sedols = self.fetch_symb('SEDOL')
        return self.sedols

    def fetch_data(self, tr_list, start='1983-01-01', end='2023-06-30', period='FY') -> pd.DataFrame:
        """
        Fetches and returns data in pandas DataFrame without error.
        This DataFrame is stored in this instance, so to view previous fetches, use show_history() function.
        :param tr_list: list-like data of TR fields (e.g. ['TR.SharesOutstanding', 'TR.Revenue']
        :param start: the first date to fetch data, in the format 'YYYY-MM-DD' (e.g. '1983-01-01')
        :param end: the last date to fetch data, in the format 'YYYY-MM-DD' (e.g. '2020-12-31')
        :param period: period of which the data is fetched. 'FY' by default (e.g. 'FY', 'FS', 'FQ', 'daily')
        :return: DataFrame that contains RIC codes in 'Instrument' column and other data in columns named after TR field names
        """
        if len(start.split('-')) != 3 or len(end.split('-')) != 3:
            raise ValueError('start and end values should be given in the format of "YYYY-MM-DD". ')
        if period not in ['FY', 'FS', 'FQ', 'daily']:
            raise ValueError('period value should be given as either "FY", "FS", "FQ", or "daily". ')
        if type(tr_list) != list:
            tr_list = [item for item in tr_list]

        tr_and_date_list = tr_list + [item + '.CALCDATE' for item in tr_list]
        tr_and_date_list.sort()
        fields = []
        [fields.append(ek.TR_Field(tr_item)) for tr_item in tr_and_date_list]

        datedict = {"SDate": start, "EDate": end, 'Curn': 'GBP', 'Period': period + '0', 'Frq': period}

        try:
            df, err = ek.get_data(self.ric_codes, fields, parameters=datedict, field_name=True)
            self._raw_data_list.append(df)
            for col in df.columns:
                if col.count('.') < 2:  # if not calcdate
                    continue
                df[col] = df[col].astype(str)
                df.loc[:, col] = df.loc[:, col].str[:10]
                df[col].replace("<NA>", float('NaN'), inplace=True)
                df[col].replace("", float('NaN'), inplace=True)
            self._data_list.append(df)
            return df
        except ek.EikonError as eke:
            print('Error code: ', eke.code)
            if eke.code in _not_my_fault:
                # sleep(1)
                return self.fetch_data(tr_list, start=start, end=end, period=period)
            elif eke.code == 429:
                beep()
                raise RuntimeError('Code 429: reached API calls limit')
            else:
                raise RuntimeError('An error occurred; read the message above!')

    def fetch_price_data(self, start='2010-01-01', end='2023-06-30') -> pd.DataFrame:
        """
        Fetches and returns ohlc+v data in pandas DataFrame without error.
        This DataFrame is stored in this instance, so to view previous fetches, use show_history() function.
        :param start: the first date to fetch data, in the format 'YYYY-MM-DD' (e.g. '1983-01-01')
        :param end: the last date to fetch data, in the format 'YYYY-MM-DD' (e.g. '2020-12-31')
        :return: DataFrame that contains RIC codes in 'Instrument' column and other data in columns named after TR field names
        """
        if len(start.split('-')) != 3 or len(end.split('-')) != 3:
            raise ValueError('start and end values should be given in the format of "YYYY-MM-DD". ')

        tr_list = ['TR.OPENPRICE', 'TR.HIGHPRICE', 'TR.LOWPRICE', 'TR.CLOSEPRICE', 'TR.Volume']
        tr_and_date_list = tr_list + [item + '.CALCDATE' for item in tr_list]
        tr_and_date_list.sort()
        fields = []
        [fields.append(ek.TR_Field(tr_item)) for tr_item in tr_and_date_list]

        datedict = {"SDate": start, "EDate": end, 'Curn': 'GBP'}

        df, err = ek.get_data(self.ric_codes, fields, parameters=datedict, field_name=True)
        self._raw_data_list.append(df)
        for col in df.columns:
            if col[-9:] != '.CALCDATE':
                continue
            df[col] = df[col].astype(str)
            df.loc[:, col] = df.loc[:, col].str[:10]
            df[col].replace("<NA>", float('NaN'), inplace=True)
            df[col].replace("", float('NaN'), inplace=True)
        self._data_list.append(df)
        return df

    def get_history(self, index=None, raw=False) -> list:
        """
        Returns previous fetch(es) of data.
        :param index: The index_data of history (e.g. -1 -> last fetch, 0 -> first fetch, [0, -1] -> first and last fetch, None -> all)
        :param raw: True if you want to fetch the history of raw data
        :return: list of dataframe(s)
        """
        ret_list = self._raw_data_list if raw else self._data_list
        if index is None:
            return ret_list
        elif type(index) is list:
            return [ret_list[i] for i in index]
        elif type(index) is int:
            return [ret_list[index]]

    def comp_specific_data(self, ric_code, raw=False) -> pd.DataFrame:
        """
        Returns a DataFrame whose Instrument column is ric_code from the last fetch
        :param ric_code: the code of the firm to get
        :param raw: True if you want to fetch from the last raw data
        :return: DataFrame whose Instrument column is ric_code from the last fetch
        """
        if len(self.get_history(raw=raw)) < 1:
            raise IndexError('No data has been fetched yet.')
        last_df = self.get_history(-1, raw=raw)[0]
        return last_df[last_df['Instrument'] == ric_code]

    def set_history(self, dataframes, raw=False) -> None:
        """
        Sets the list of fetch history to the given dataframes.
        :param dataframes: the list to set history as.
        :param raw: True if you want to set the history of raw data
        :return: None
        """
        if type(dataframes) is list:
            self._data_list = dataframes if not raw else self._data_list
            self._raw_data_list = dataframes if raw else self._raw_data_list
        elif type(dataframes) is pd.DataFrame:
            self._data_list = [dataframes] if not raw else self._data_list
            self._raw_data_list = [dataframes] if raw else self._raw_data_list
        else:
            raise TypeError("The parameter given to set_history() should be a list or a pd.DataFrame.")

    def clear_history(self, raw=False) -> None:
        """
        Clears all data history
        :param raw: True if you want to clear the history of raw data
        :return: None
        """
        self.set_history([], raw=raw)


if __name__ == '__main__':
    ans = input('You are running myEikon.py. Do you wish to continue? [y/n]')
    if ans == 'y':
        test = Companies(['AZN.L', 'HSBA.L'])
        data = test.fetch_data(['TR.IssuerRating', 'TR.IssuerRating.calcdate'], start='2020-01-01', end='2023-06-30', )
        print(data)
