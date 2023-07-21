import pandas as pd
import math
import time
# import myEikon as dbb
import eikon as ek
import os
import numpy as np

"""
print(ek.get_data("AZN.L", [{"TR.SharesOutstanding": {"SDate": "2010-01-01", "EDate": "2020-12-31", "Currency": "USD"}},
                            {"TR.NormIncomeBeforeTaxes": {"SDate": "2010-01-01", "EDate": "2020-12-31", "Currency": "USD"}},
                            {"TR.NormIncomeBeforeTaxes": {"SDate": "2010-01-01", "EDate": "2020-12-31"}},
                            "Currency"])[0].transpose())
"""
"""
shrout = ek.get_data(["AZN.L", "AAPL.O"], 'TR.SharesOutstanding', {"SDate": "2010-01-01", "EDate": "2020-12-31"})[0]
aznshrout = shrout[shrout["Instrument"] == "AZN.L"]
print(aznshrout)
prices = ek.get_timeseries(["AZN.L", "AAPL.O"], start_date="2010-01-01", end_date="2020-12-31")
aznprice = prices["AZN.L"]
print(aznprice)
"""

"""
print(ek.get_data(["AZN.L", "AAPL.O", "HSBA.L"], "Currency")[0])
"""

'''
asdf = pd.read_csv('https://www.jamesd002.com/file/close.csv')
print(asdf)
'''

'''
txtlist = ['230710_161343_log.txt', '230710_192414_log.txt', '230710_195715_log.txt', '230710_200219_log.txt']
shits = []
for txtfile in txtlist:
    with open('./files/' + txtfile, 'r') as f:
        loglines = [line.rstrip() for line in f]

    loglines = [i for i in loglines if len(i) > 0 and i[0] == "!"]
    loglines = [i.replace('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!Empty Dataset for ', '').replace(
        '!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!', '') for i in loglines]
    [shits.append(item) for item in loglines]
print(shits)

with open('./files/no_data.txt', 'w') as f:
    for line in shits:
        f.write(f"{line}\n")
'''

'''
shits = os.listdir('./files/price_stuff/price_data_fixed/')
shits = [i[:-4] for i in shits]
df = pd.read_csv('./files/comp_list/comp_list.csv')
df = df[df['RIC1(ticker)'].isin(shits)]
df.to_csv('./files/comp_list/done.csv', indices=False)
print(df['ISIN'].isna().sum(), '/', len(df), ',', len(shits))
'''

'''
close_df = pd.read_csv('./files/price_stuff/price_data_merged/close.csv')
close_df = close_df.set_index('Date')
notna_df = close_df.notna()
df_rand = pd.DataFrame(np.random.randn(*close_df.shape), columns=close_df.columns)
df_rand = df_rand.set_index(close_df.indices)
df_rand = df_rand.abs()*100
shrout_df = np.round(df_rand, 2) * notna_df
shrout_df.to_csv('./files/by_data/shrout.csv')
mve_df = shrout_df * close_df
mve_df.to_csv('./files/by_data/mve.csv')
'''

''' calculating business_days.csv
close_df = pd.read_csv('files/by_data/secd/close.csv')[
    ['datadate', 'AZN.L']]  # AZN is just a placeholder to keep df not series
close_df['YYYY'] = close_df['datadate'].apply(lambda x: str(x)[:4])
close_df['MM'] = close_df['datadate'].apply(lambda x: str(x)[4:6])
close_df['DD'] = close_df['datadate'].apply(lambda x: str(x)[-2:])
close_df['YYYYMMDD'] = close_df['datadate']
close_df['YYYY-MM-DD'] = close_df['YYYY'] + '-' + close_df['MM'] + '-' + close_df['DD']
close_df = close_df.drop(['AZN.L', 'datadate'], axis=1)
close_df.to_csv('files/metadata/business_days.csv', indices=False)
# '''

'''
files = os.listdir('files/fund_data/FY/')
for afile in files:
    thedf = pd.read_csv('files/fund_data/FY/'+afile)
    thedf = thedf.fillna(0)
    thedf.to_csv('files/fund_data/FY/'+afile, indices=False)
    print(f'{afile} done!')
# '''

''' pickle test
asdf = pd.read_csv('files/comp_list/comp_list.csv')
asdf['RIC1(ticker)'] = asdf['RIC'].apply(lambda x: x.split('.L')[0])
asdf['Company Name'] = asdf['Company Name'].astype(str)
asdf['RIC'] = asdf['RIC'].astype(str)
asdf['RIC1(ticker)'] = asdf['RIC1(ticker)'].astype(str)
asdf['delisted MM'] = asdf['delisted MM'].astype('int8')
asdf['delisted YY'] = asdf['delisted YY'].astype('int16')
asdf['ISIN'] = asdf['ISIN'].astype(str)
asdf['CUSIP'] = asdf['CUSIP'].astype(str)
asdf['SEDOL'] = asdf['SEDOL'].astype(str)
asdf.to_pickle('files/comp_list/comp_list.pickle')
# '''

asdf = pd.read_csv('files/fund_data/FY/AZN-L.csv')
asdf['count'] = (pd.to_datetime(asdf['datadate']) - pd.to_datetime(asdf['datadate']).min()).dt.days // 365 + 1
print(asdf)

