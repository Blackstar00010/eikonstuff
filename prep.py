import pandas as pd
import math
import myEikon as dbb

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
screen.to_csv("./files/comp_list/screen_230706.csv", index=False)
# columns: Company Name,ISIN,CUSIP (extended),SEDOL Code,RIC

# prepare for merging listed and delisted
listed = pd.read_csv('./files/comp_list/screen_230706.csv')[['Company Name', 'RIC']]
delisted = pd.read_csv("./files/comp_list/delisted.csv")[[
    'Name (or Code)', 'RIC', 'RIC1(ticker)', 'RIC2(exchange)', 'delisted mm', 'delisted yy']]
delisted = delisted[delisted['RIC2(exchange)'] == 'L']  # Strictly LSE. Not 'Lp' (probably preferred, and 'TRE' (idk)
delisted = delisted.drop('RIC2(exchange)', axis=1)  # Now that all are 'L' type, not needed

listed['RIC1(ticker)'] = listed['RIC'].str.replace('.L', '')
listed['delisted MM'] = 0  # not delisted
listed['delisted YY'] = 0  # not delisted

delisted = delisted.rename(
    columns={'Name (or Code)': 'Company Name', 'delisted mm': 'delisted MM', 'delisted yy': 'delisted YY'})

# merging two dataframes
all_firms = pd.concat([listed, delisted], axis=0)

comp_list = all_firms["RIC"].to_list()
total_length = len(comp_list)

divide_by = 5000
already_have_until = 0  # already have isin, cusip, sedol until this row
if already_have_until > 0:
    df_already_have = pd.read_csv('./files/comp_list/comp_list_all.csv')[['ISIN', 'CUSIP', 'SEDOL']]
    comp_list.loc[0:already_have_until, 'ISIN'] = df_already_have["ISIN"].tolist()
    comp_list.loc[0:already_have_until, 'CUSIP'] = df_already_have["CUSIP"].tolist()
    comp_list.loc[0:already_have_until, 'SEDOL'] = df_already_have["SEDOL"].tolist()

for i in range(math.ceil((total_length - already_have_until) // divide_by)):
    smaller_list = comp_list[already_have_until + i * divide_by: already_have_until + i * divide_by + divide_by]
    companies_db = dbb.Companies(smaller_list)

    all_firms.loc[already_have_until + i * divide_by: already_have_until + i * divide_by + divide_by,
                  "ISIN"] = companies_db.fetch_isin()
    all_firms.loc[already_have_until + i * divide_by: already_have_until + i * divide_by + divide_by,
                  "CUSIP"] = companies_db.fetch_cusip()
    all_firms.loc[already_have_until + i * divide_by: already_have_until + i * divide_by + divide_by,
                  "SEDOL"] = companies_db.fetch_sedol()

    all_firms.to_csv('./files/comp_list/comp_list_all.csv', index=False)

# delete duplicates in Company Name
duplicates = all_firms[all_firms.duplicated(subset=["Company Name"], keep=False)]
duplicates = duplicates.sort_values(by="RIC1(ticker)")
duplicates.to_csv('./files/comp_list/comp_list_dups.csv', index=False)

# without those duplicates
no_duplicates = all_firms.drop_duplicates(subset=["Company Name"], keep="last")
no_duplicates.to_csv('./files/comp_list/comp_list.csv', index=False)
