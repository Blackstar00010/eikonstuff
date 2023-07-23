import pandas as pd
import math
import myEikon as mek

'''
at files/comp_list/,
1. screen_230706.xlsx -> screen_230706.csv
2. screen_230706.csv + delisted.csv + isin, cusip, sedol -> comp_list_all.csv
3. comp_list_all.csv 's duplicates -> comp_list_dups.csv
4. comp_list_all.csv 's non-dups -> comp_list.csv
'''

# currently listed companies
screen = pd.read_excel("./files/comp_list/screen_230706.xlsx")
screen['Company Name'] = screen['Company Name'].fillna('')
screen = screen[~screen["Company Name"].str.contains("ETF")]  # remove ETFs
screen = screen[~screen["Company Name"].str.contains("Lyxor Smart Overnight Return - IE")]
screen = screen[screen["ETP Basket header date"].isna()]
screen = screen.drop("ETP Basket header date", axis=1)  # remove an empty column
screen = screen[screen["RIC"].notna()]  # remove empty rows
screen = screen.drop("Unnamed: 6", axis=1)  # same as ISIN
screen = screen.drop("Identifier", axis=1)  # same as RIC
screen = screen.drop('Price Close\n(2023-06-20, GBP)', axis=1)  # useless
screen = screen.drop('Exchange Name', axis=1)  # always LONDON STOCK EXCHANGE
screen = screen[~screen['RIC'].isin(['MGPL.L', 'FTSV.L'])]  # freshly deleted in July 2023
screen.to_csv("./files/comp_list/screen_230706.csv", index=False)
# columns: Company Name,ISIN,CUSIP (extended),SEDOL Code,RIC

# prepare for merging listed and delisted
listed = pd.read_csv('./files/comp_list/screen_230706.csv')[['Company Name', 'RIC']]
delisted = pd.read_csv("./files/comp_list/delisted.csv")[[
    'Name (or Code)', 'RIC', 'RIC1(ticker)', 'RIC2(exchange)', 'delisted mm', 'delisted yy']]
delisted = delisted[delisted['RIC2(exchange)'] == 'L']  # Strictly LSE. Not 'Lp' (probably preferred, and 'TRE' (idk)
delisted = delisted.drop('RIC2(exchange)', axis=1)  # Now that all are 'L' type, not needed
mgpm = ['Medica Group PLC', 'MGPM.L^G23', 'MGPM', '7', '2023']  # freshly delisted in July 2023
ftsv = ['Foresight Solar & Technology VCT PLC', 'FTSV.L^G23', 'FTSV', '7', '2023']  # freshly delisted in July 2023
delisted.loc[delisted.shape[0]] = mgpm
delisted.loc[delisted.shape[0]] = ftsv

listed['RIC1(ticker)'] = listed['RIC'].str.replace('.L', '')
listed['delisted MM'] = 0  # not delisted
listed['delisted YY'] = 0  # not delisted

delisted = delisted.rename(
    columns={'Name (or Code)': 'Company Name', 'delisted mm': 'delisted MM', 'delisted yy': 'delisted YY'})

# merging two dataframes
all_firms = pd.concat([listed, delisted], axis=0)
all_firms = all_firms.reset_index(drop=True)

already_have_until = len(all_firms)-1  # already have isin, cusip, sedol until this row
if already_have_until > 0:
    df_already_have = pd.read_pickle('./files/comp_list/comp_list_all.pickle')[['ISIN', 'CUSIP', 'SEDOL']]
    all_firms.loc[0:already_have_until, 'ISIN'] = df_already_have.loc[0:already_have_until, "ISIN"]
    all_firms.loc[0:already_have_until, 'CUSIP'] = df_already_have.loc[0:already_have_until, "CUSIP"]
    all_firms.loc[0:already_have_until, 'SEDOL'] = df_already_have.loc[0:already_have_until, "SEDOL"]

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

    all_firms.to_csv('./files/comp_list/comp_list_all.csv', index=False)
    all_firms.to_pickle('./files/comp_list/comp_list_all.pickle')
    print(f'{i} / {len(all_firms)}')

# delete duplicates in Company Name
duplicates = all_firms[all_firms.duplicated(subset=["Company Name"], keep=False)]
duplicates = duplicates.sort_values(by="RIC1(ticker)")
duplicates.to_csv('./files/comp_list/comp_list_dups.csv', index=False)

# without those duplicates
no_duplicates = all_firms.drop_duplicates(subset=["Company Name"], keep="last")
no_duplicates.to_csv('./files/comp_list/comp_list.csv', index=False)
no_duplicates.to_pickle('./files/comp_list/comp_list.pickle')
