import eikon as ek
import eikon.eikonError as eke
import pandas as pd
import numpy as np

apikey = "7fb0e788b2ff42c2823e80933fde4d28158c74f4"
ek.set_app_key(apikey)

if __name__ == "__main__":
    print("hello")


class Companies:
    def __init__(self, comp_codes, code_type="RIC", fetch_isin=True, fetch_cusip=False, fetch_sedol=False):
        """
        :param comp_codes: list of codes (usually RIC codes which are used to search firms on Eikon search bar)
        :param code_type: one of "RIC", "ISIN", "CUSIP", "SEDOL"
        """
        if code_type == "RIC":
            self.ric_codes = comp_codes
        else:
            self.ric_codes = ek.get_symbology(comp_codes, from_symbol_type=code_type, to_symbol_type="RIC")[
                "RIC"].to_list()

        if fetch_isin:
            self.isins = ek.get_symbology(self.ric_codes, from_symbol_type="RIC", to_symbol_type="ISIN")[
                "ISIN"].to_list()
            if self.isins == "No best match available":
                self.isins = np.zeros(len(self.ric_codes))
        else:
            self.isins = np.zeros(len(self.ric_codes))
        if fetch_cusip:
            self.cusips = ek.get_symbology(self.ric_codes, from_symbol_type="RIC", to_symbol_type="CUSIP")[
                "CUSIP"].to_list()
            if self.cusips == "No best match available":
                self.cusips = np.zeros(len(self.ric_codes))
        else:
            self.cusips = np.zeros(len(self.ric_codes))
        if fetch_sedol:
            self.sedols = ek.get_symbology(self.ric_codes, from_symbol_type="RIC", to_symbol_type="SEDOL")[
                "SEDOL"].to_list()
            if self.sedols == "No best match available":
                self.sedols = np.zeros(len(self.ric_codes))
        else:
            self.sedols = np.zeros(len(self.ric_codes))

        # self.ipodates = ek.get_data(self.ric_codes, "TR.IPODate")[0]["IPO Date"].tolist()

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
            return ek.get_timeseries(self.ric_codes, start_date=str(dec) + "-01-01", end_date=str(dec + 9) + "-12-31",
                                     corax=corax)
        except eke.EikonError:
            print(f"{self.ric_codes}: No data available for {dec}-{dec + 9}")
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

    def save_price(self):
        """
        saves the firm's price/volume data as a csv file under ./files/history/ folder
        :return: None
        """
        self.price_data.to_csv(f"./files/history/{self.ric_codes[:-2]}.csv")
