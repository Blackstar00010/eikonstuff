import pandas as pd
import Misc.useful_stuff as us
# import myEikon as dbb

""" Testing TR Fields
print(ek.get_data("AZN.L", [{"TR.SharesOutstanding": {"SDate": "2010-01-01", "EDate": "2020-12-31", "Currency": "USD"}},
                            {"TR.NormIncomeBeforeTaxes": {"SDate": "2010-01-01", "EDate": "2020-12-31", "Currency": "USD"}},
                            {"TR.NormIncomeBeforeTaxes": {"SDate": "2010-01-01", "EDate": "2020-12-31"}},
                            "Currency"])[0].transpose())
"""
""" Testing how to fetch SharesOutstanding
shrout = ek.get_data(["AZN.L", "AAPL.O"], 'TR.SharesOutstanding', {"SDate": "2010-01-01", "EDate": "2020-12-31"})[0]
aznshrout = shrout[shrout["Instrument"] == "AZN.L"]
print(aznshrout)
prices = ek.get_timeseries(["AZN.L", "AAPL.O"], start_date="2010-01-01", end_date="2020-12-31")
aznprice = prices["AZN.L"]
print(aznprice)
"""
'''
asdf = pd.read_csv('https://www.jamesd002.com/file/close.csv')
print(asdf)
'''
'''
close_df = pd.read_csv('./data/price_stuff/price_data_merged/close.csv')
close_df = close_df.set_index('Date')
notna_df = close_df.notna()
df_rand = pd.DataFrame(np.random.randn(*close_df.shape), columns=close_df.columns)
df_rand = df_rand.set_index(close_df.index_data)
df_rand = df_rand.abs()*100
shrout_df = np.round(df_rand, 2) * notna_df
shrout_df.to_csv('./data/processed/shrout.csv')
mve_df = shrout_df * close_df
mve_df.to_csv('./data/processed/mve.csv')
'''
''' calculating business_days.csv
close_df = pd.read_csv('data/processed/secd/close.csv')[
    ['datadate', 'AZN.L']]  # AZN is just a placeholder to keep df not series
close_df['YYYY'] = close_df['datadate'].apply(lambda x: str(x)[:4])
close_df['MM'] = close_df['datadate'].apply(lambda x: str(x)[4:6])
close_df['DD'] = close_df['datadate'].apply(lambda x: str(x)[-2:])
close_df['YYYYMMDD'] = close_df['datadate']
close_df['YYYY-MM-DD'] = close_df['YYYY'] + '-' + close_df['MM'] + '-' + close_df['DD']
close_df = close_df.drop(['AZN.L', 'datadate'], axis=1)
close_df.to_csv('data/metadata/business_days.csv', index_data=False)
# '''
'''
data = os.listdir('data/preprocessed/FY/')
for afile in data:
    thedf = pd.read_csv('data/preprocessed/FY/'+afile)
    thedf = thedf.fillna(0)
    thedf.to_csv('data/preprocessed/FY/'+afile, index_data=False)
    print(f'{afile} done!')
# '''
''' pickle test
asdf = pd.read_csv('data/comp_list/comp_list.csv')
asdf['RIC1(ticker)'] = asdf['RIC'].apply(lambda x: x.split('.L')[0])
asdf['Company Name'] = asdf['Company Name'].astype(str)
asdf['RIC'] = asdf['RIC'].astype(str)
asdf['RIC1(ticker)'] = asdf['RIC1(ticker)'].astype(str)
asdf['delisted MM'] = asdf['delisted MM'].astype('int8')
asdf['delisted YY'] = asdf['delisted YY'].astype('int16')
asdf['ISIN'] = asdf['ISIN'].astype(str)
asdf['CUSIP'] = asdf['CUSIP'].astype(str)
asdf['SEDOL'] = asdf['SEDOL'].astype(str)
asdf.to_pickle('data/comp_list/comp_list.pickle')
# '''
'''
file_names = os.listdir('data/preprocessed/FY/')
for afile in file_names:
    asdf = pd.read_csv('data/preprocessed/FY/'+afile)
    asdf['count'] = (pd.to_datetime(asdf['datadate']) - pd.to_datetime(asdf['datadate']).min()).dt.days // 365 + 1
    asdf.to_csv('data/preprocessed/FY/'+afile, index=False)
'''
''' Fixing  ticker1.csv data that had no -L^MYY in their names
price_data_dir = 'data/price_stuff/price_data/'
comp_list = pd.read_pickle('data/comp_list/comp_list.pickle')[['RIC', 'RIC1(ticker)']]
dups = pd.read_csv('data/comp_list/comp_list_dup_ric1.csv')[['RIC', 'RIC1(ticker)']]
dupduplist = []
file_names = os.listdir(price_data_dir)
for afile in file_names:
    if '-L' not in afile:
        # ric1 = afile[:-4]
        # ric = comp_list[comp_list['RIC1(ticker)'] == ric1]['RIC']
        # if len(ric) > 1:
        #     print(ric1 + ' ' + str([aric for aric in ric]))
        #     [dupduplist.append(aric) for aric in ric if aric.replace('.', '-') + '.csv' not in price_data_dir]
        # elif len(ric) == 1:
        #     ric = ric.values[0]
        #     ric = ric.replace('.', '-')
        #     if ric+'.csv' in file_names:
        #         os.remove(price_data_dir + afile)
        #     else:
        #         os.rename(price_data_dir + afile, price_data_dir + ric + '.csv')
        # else:
        #     os.remove(price_data_dir + afile)
        os.remove(price_data_dir + afile)
# dupduplist = pd.DataFrame(dupduplist).to_csv('data/comp_list/unfetched_0723.csv')
'''
''' Removing dupliacted lines from no_data.txt and no_timestamp.txt
with open('data/comp_list/no_data.txt', 'r') as f:
    to_edit = [line.rstrip() for line in f]
to_edit = sorted(set(to_edit))
with open('data/comp_list/no_data.txt', 'w') as f:
    for line in to_edit:
        f.write(f"{line}\n")
with open('data/comp_list/no_timestamp.txt', 'r') as f:
    to_edit = [line.rstrip() for line in f]
to_edit = sorted(set(to_edit))
with open('data/comp_list/no_timestamp.txt', 'w') as f:
    for line in to_edit:
        f.write(f"{line}\n")
# '''
'''
price_date_col = pd.read_csv('../data/price_stuff/adj_price_data_merged/adj_close.csv')
price_date_col = price_date_col[us.date_col_finder(price_date_col, '')]
og_price_col = pd.read_csv('../data/metadata/business_days.csv')
og_price_col = og_price_col['YYYY-MM-DD']
print(price_date_col[~price_date_col.isin(og_price_col)])
the_dir = 'files/fund_data/FQ/'
for afile in us.listdir(the_dir):
    df = pd.read_csv(the_dir + afile)
    if 'fatbq' in df.columns:
        df = df.loc[:, df.columns.drop(['fatbq'])]
    if 'fatbq_x' in df.columns:
        df = df.loc[:, df.columns.drop(['fatbq_x', 'fatbq_y'])]
    df.to_csv(the_dir + afile, index=False)
'''

close_df = pd.read_csv('data//price_data_merged/close.csv')
