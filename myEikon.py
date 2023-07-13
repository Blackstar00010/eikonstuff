import pandas as pd
# import numpy as np
import eikon as ek

apikey = "7fb0e788b2ff42c2823e80933fde4d28158c74f4"
ek.set_app_key(apikey)


class Company:
    def __init__(self, comp_codes):
        """
        :param comp_codes: an RIC code as a str
        """
        self.ric_code = comp_codes
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

    def fetch_isin(self):
        """
        Returns list of ISIN Codes
        :return: list of ISIN codes
        """
        self.isins = ek.get_symbology(self.ric_codes, from_symbol_type="RIC", to_symbol_type="ISIN")[
            "ISIN"].to_list()

    def fetch_cusip(self):
        """
        Returns list of CUSIP Codes
        :return: list of CUSIP codes
        """
        self.cusips = ek.get_symbology(self.ric_codes, from_symbol_type="RIC", to_symbol_type="CUSIP")[
            "CUSIP"].to_list()

    def fetch_sedol(self):
        """
        Returns list of SEDOL Codes
        :return: list of SEDOL Codes
        """
        self.sedols = ek.get_symbology(self.ric_codes, from_symbol_type="RIC", to_symbol_type="SEDOL")[
            "SEDOL"].to_list()

    def fetch_data(self, tr_list, start='1983-01-01', end='2020-12-31'):
        """
        Fetches and returns data in pandas DataFrame without error. This DataFrame is stored in this instance, so to
        view previous fetches, use show_history() function.
        :param tr_list: list of TR fields (e.g. ['TR.SharesOutstanding', 'TR.Revenue']
        :param start: the first date to fetch data, in the format 'YYYY-MM-DD' (e.g. '1983-01-01')
        :param end: the last date to fetch data, in the format 'YYYY-MM-DD' (e.g. '2020-12-31')
        :return: DataFrame that contains RIC codes in 'Instrument' column and other data in columns named after
        TR field names
        """
        fields = []
        [fields.append(ek.TR_Field(tr_item)) for tr_item in tr_list]

        datedict = {"SDate": start, "EDate": end}

        df = ek.get_data(self.ric_codes, fields, parameters=datedict, field_name=True)
        self._data_list.append(df)
        return df

    def show_history(self, index=None):
        """
        Returns previous fetch(es) of data.
        :param index: The index of history
        (e.g. -1 -> last fetch, 0 -> first fetch, [0, -1] -> first and last fetch, None -> all)
        :return: list of dataframe(s)
        """
        if index is None:
            return self._data_list
        return [self._data_list[i] for i in index]

    def comp_specific_data(self, ric_code):
        """
        Returns a DataFrame whose Instrument column is ric_code from the last fetch
        :param ric_code: the code of the firm to get
        :return: DataFrame whose Instrument column is ric_code from the last fetch
        """
        if len(self.show_history()) < 1:
            raise IndexError('No data fetched yet.')
        last_df = self.show_history(-1)[0]
        return last_df[last_df['Instrument'] == ric_code]
