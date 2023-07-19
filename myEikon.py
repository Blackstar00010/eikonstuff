import pandas as pd
import numpy as np
import eikon as ek

apikey = "7fb0e788b2ff42c2823e80933fde4d28158c74f4"
ek.set_app_key(apikey)


class Company:
    def __init__(self, comp_code):
        """
        :param comp_code: an RIC code as a str
        """
        self.ric_code = comp_code
        self.price_data = pd.DataFrame()

    def fetch_decade(self, dec, adj=False):
        """
        fetches price/volume data of a decade and returns as a dataframe.
        :param adj: adjusted price data if True, unadjusted price if False.
        :param dec: start year an integer. (e.g. 2020)
        :return: dataframe of columns HIGH, LOW, CLOSE, OPEN, COUNT, VOLUME, SharesOutstanding.
        """
        corax = 'adjusted' if adj else 'unadjusted'
        try:
            return ek.get_timeseries(self.ric_code, start_date=str(dec) + "-01-01", end_date=str(dec + 9) + "-12-31",
                                     corax=corax)
        except ek.EikonError:
            print(f"{self.ric_code}: No data available for {dec}-{dec + 9}")
            return pd.DataFrame()

    def fetch_price(self, overwrite=False, delisted=2024, adj=False):
        """
        fetches price/volume data and returns as a dataframe.
        :param adj: adjusted price data if True, unadjusted price if False.
        :param delisted: the year the firm got delisted
        :param overwrite: overwrites price data dataframe if it is not empty
        :return: dataframe of columns HIGH, LOW, CLOSE, OPEN, COUNT, VOLUME.
        """

        if overwrite or self.price_data.empty:
            startyear = 1983
            df = self.fetch_decade(startyear, adj=adj)
            startyear += 10
            while startyear < delisted:
                new_df = self.fetch_decade(startyear, adj=adj)
                df = pd.concat([df, new_df], axis=0)
                startyear += 10
            self.price_data = df
        return self.price_data


class Companies:
    def __init__(self, code_list):
        """
        :param code_list: list (or ndarray) of RIC codes
        """
        self.ric_codes = code_list
        self.isins = []
        self.cusips = []
        self.sedols = []
        self._data_list = []
        self._raw_data_list = []

    def fetch_isin(self):
        """
        Returns list of ISIN Codes
        :return: list of ISIN codes
        """
        self.isins = ek.get_symbology(self.ric_codes, from_symbol_type="RIC", to_symbol_type="ISIN")[
            "ISIN"].to_list()
        return self.isins

    def fetch_cusip(self):
        """
        Returns list of CUSIP Codes
        :return: list of CUSIP codes
        """
        self.cusips = ek.get_symbology(self.ric_codes, from_symbol_type="RIC", to_symbol_type="CUSIP")[
            "CUSIP"].to_list()
        return self.cusips

    def fetch_sedol(self):
        """
        Returns list of SEDOL Codes
        :return: list of SEDOL Codes
        """
        self.sedols = ek.get_symbology(self.ric_codes, from_symbol_type="RIC", to_symbol_type="SEDOL")[
            "SEDOL"].to_list()
        return self.sedols

    def fetch_data(self, tr_list, start='1983-01-01', end='2023-06-30', period='FY'):
        """
        Fetches and returns data in pandas DataFrame without error.
        This DataFrame is stored in this instance, so to view previous fetches, use show_history() function.
        :param tr_list: list of TR fields (e.g. ['TR.SharesOutstanding', 'TR.Revenue']
        :param start: the first date to fetch data, in the format 'YYYY-MM-DD' (e.g. '1983-01-01')
        :param end: the last date to fetch data, in the format 'YYYY-MM-DD' (e.g. '2020-12-31')
        :param period: period of which the data is fetched. 'FY' by default (e.g. 'FY', 'FS', 'FQ')
        :return: DataFrame that contains RIC codes in 'Instrument' column and other data in columns named after TR field names
        """
        if len(start.split('-')) != 3 or len(end.split('-')) != 3:
            raise ValueError('start and end values should be given in the format of "YYYY-MM-DD". ')
        if period not in ['FY', 'FS', 'FQ']:
            raise ValueError('period value should be given as either "FY", "FS", or "FQ". ')

        tr_and_date_list = tr_list + [item + '.CALCDATE' for item in tr_list]
        tr_and_date_list.sort()
        fields = []
        [fields.append(ek.TR_Field(tr_item)) for tr_item in tr_and_date_list]

        datedict = {"SDate": start, "EDate": end, 'Curn': 'GBP', 'Frq': period}

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

    def fetch_price_data(self, start='2010-01-01', end='2023-06-30'):
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

    def get_history(self, index=None, raw=False):
        """
        Returns previous fetch(es) of data.
        :param index: The index of history (e.g. -1 -> last fetch, 0 -> first fetch, [0, -1] -> first and last fetch, None -> all)
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

    def comp_specific_data(self, ric_code, raw=False):
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

    def set_history(self, dataframes, raw=False):
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

    def clear_history(self, raw=False):
        """
        Clears all data history
        :param raw: True if you want to clear the history of raw data
        :return: None
        """
        self.set_history([], raw=raw)
