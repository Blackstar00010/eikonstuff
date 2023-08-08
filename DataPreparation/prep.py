import pandas as pd
import os
import myEikon as mek

'''
at ../data/metadata/comp_list/,
1. screen_230706.xlsx -> screen_230706.csv
2. screen_230706.csv + delisted.csv + isin, cusip, sedol -> comp_list_all.csv
3. comp_list_all.csv 's duplicates -> comp_list_dup_cname.csv, comp_list_dup_ric1.csv
4. comp_list_all.csv 's non-dups -> comp_list.csv
'''

comp_list_dir = '../data/metadata/comp_list/'

# currently listed companies
screen = pd.read_excel(os.path.join(comp_list_dir, 'screen_230706.xlsx'))
screen['Company Name'] = screen['Company Name'].fillna('')
screen = screen[~screen['Company Name'].str.contains('ETF')]  # remove ETFs
screen = screen[~screen['Company Name'].str.contains('Lyxor Smart Overnight Return - IE')]
screen = screen[screen['ETP Basket header date'].isna()]
screen = screen.drop('ETP Basket header date', axis=1)  # remove an empty column
screen = screen[screen['RIC'].notna()]  # remove empty rows
screen = screen.drop('Unnamed: 6', axis=1)  # same as ISIN
screen = screen.drop('Identifier', axis=1)  # same as RIC
screen = screen.drop('Price Close\n(2023-06-20, GBP)', axis=1)  # useless
screen = screen.drop('Exchange Name', axis=1)  # always LONDON STOCK EXCHANGE

g23_list = pd.read_excel(os.path.join(comp_list_dir, 'delisted_G23.xlsx'))['RIC']  # freshly deleted in July 2023
g23_list = g23_list.apply(lambda x: x[:-4])
g23_df = screen[screen['RIC'].isin(g23_list)][['Company Name', 'RIC']]
screen = screen[~screen['RIC'].isin(g23_list)]
screen.to_csv(comp_list_dir + 'screen_230706.csv', index=False)
# columns: Company Name,ISIN,CUSIP (extended),SEDOL Code,RIC

# renaming existing data containing the G23 companies
dirs = ['data/preprocessed/FQ/', 'data/preprocessed/FS/', 'data/preprocessed/FY/',
        'data/price_stuff/adj_price_data/', 'data/price_stuff/adj_price_data_fixed/',
        'data/price_stuff/price_data/', 'data/price_stuff/price_data_fixed/']
for ric_wo_g23 in g23_list:
    ric_wo_g23 = ric_wo_g23.replace('.', '-')
    ric_w_g23 = ric_wo_g23 + '^G23'
    for adir in dirs:
        if os.path.exists(adir + ric_wo_g23 + '.csv'):
            os.rename(adir + ric_wo_g23 + '.csv', adir + ric_w_g23 + '.csv')

# updating available.csv & available.pickle
g23_dict = {aric: aric+'^G23' for aric in g23_list}
available = pd.read_pickle(comp_list_dir + 'available.pickle')
available['RIC'] = available['RIC'].replace(g23_dict)
for i in available.index:
    if available.loc[i, 'RIC'][-4:] == '^G23':
        available.loc[i, ['delisted MM', 'delisted YY']] = [7, 2023]
available.to_csv(comp_list_dir + 'available.csv', index=False)
available.to_pickle(comp_list_dir + 'available.pickle')

# prepare for merging listed and delisted
listed = pd.read_csv(comp_list_dir + 'screen_230706.csv')[['Company Name', 'RIC']]
delisted = pd.read_csv(comp_list_dir + 'delisted.csv')[[
    'Name (or Code)', 'RIC', 'RIC1(ticker)', 'RIC2(exchange)', 'delisted mm', 'delisted yy']]
delisted = delisted[delisted['RIC2(exchange)'] == 'L']  # Strictly LSE. Not 'Lp' (probably preferred, and 'TRE' (idk)
delisted = delisted.drop('RIC2(exchange)', axis=1)  # Now that all are 'L' type, not needed

g23_df['RIC1(ticker)'] = g23_df['RIC'].str.replace('.L', '')
g23_df['delisted mm'] = 7
g23_df['delisted yy'] = 2023
g23_df['RIC'] = g23_df['RIC'].apply(lambda x: x+'^G23')
g23_df = g23_df.rename(columns={'Company Name': 'Name (or Code)'})
delisted = pd.concat([delisted, g23_df], axis=0)

listed['RIC1(ticker)'] = listed['RIC'].str.replace('.L', '')
listed['delisted MM'] = 0  # not delisted
listed['delisted YY'] = 0  # not delisted

delisted = delisted.rename(
    columns={'Name (or Code)': 'Company Name',
             'delisted mm': 'delisted MM',
             'delisted yy': 'delisted YY'})

# merging two dataframes
all_firms = pd.concat([listed, delisted], axis=0)
all_firms = all_firms.reset_index(drop=True)

already_have_until = len(all_firms)-100  # already have isin, cusip, sedol until this row
if already_have_until > 0:
    df_already_have = pd.read_pickle(comp_list_dir + 'comp_list_all.pickle')[['ISIN', 'CUSIP', 'SEDOL']]
    all_firms.loc[0:already_have_until, 'ISIN'] = df_already_have.loc[0:already_have_until, 'ISIN']
    all_firms.loc[0:already_have_until, 'CUSIP'] = df_already_have.loc[0:already_have_until, 'CUSIP']
    all_firms.loc[0:already_have_until, 'SEDOL'] = df_already_have.loc[0:already_have_until, 'SEDOL']

divide_by = 1000
rics = all_firms['RIC']
for i in range(already_have_until, len(all_firms), divide_by):
    smaller_list = rics[i: i + divide_by].to_list()
    companies_db = mek.Companies(smaller_list)

    all_firms = all_firms.merge(companies_db.fetch_symb('ISIN'), on='RIC', how='left')
    all_firms['ISIN'] = all_firms['ISIN_x'].fillna(all_firms['ISIN_y'])
    all_firms = all_firms.merge(companies_db.fetch_symb('CUSIP'), on='RIC', how='left')
    all_firms['CUSIP'] = all_firms['CUSIP_x'].fillna(all_firms['CUSIP_y'])
    all_firms = all_firms.merge(companies_db.fetch_symb('SEDOL'), on='RIC', how='left')
    all_firms['SEDOL'] = all_firms['SEDOL_x'].fillna(all_firms['SEDOL_y'])
    all_firms = all_firms.drop(['ISIN_x', 'ISIN_y', 'CUSIP_x', 'CUSIP_y', 'SEDOL_x', 'SEDOL_y'], axis='columns')

    all_firms.to_csv(comp_list_dir + 'comp_list_all.csv', index=False)
    all_firms.to_pickle(comp_list_dir + 'comp_list_all.pickle')
    print(f'{i} / {len(all_firms)}')

# delete duplicates in Company Name
duplicates = all_firms[all_firms.duplicated(subset=['Company Name'], keep=False)]
duplicates = duplicates.sort_values(by='RIC1(ticker)')
duplicates.to_csv(comp_list_dir + 'comp_list_dup_cname.csv', index=False)

dup_ric_list = all_firms[all_firms.duplicated(subset='RIC1(ticker)', keep=False)]
dup_ric_list = dup_ric_list.sort_values(by='RIC1(ticker)')
dup_ric_list.to_csv(comp_list_dir + 'comp_list_dup_ric1.csv', index=False)

# without those duplicates
no_duplicates = all_firms.drop_duplicates(subset=['Company Name'], keep='last')
no_duplicates.to_csv(comp_list_dir + 'comp_list.csv', index=False)
no_duplicates.to_pickle(comp_list_dir + 'comp_list.pickle')
