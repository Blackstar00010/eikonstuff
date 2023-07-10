import pandas as pd
import math
import db_builder as dbb

# currently listed companies
screen = pd.read_excel("./files/screen_230706.xlsx")
screen['Company Name'] = screen['Company Name'].fillna('')
screen = screen[~screen["Company Name"].str.contains("ETF")]  # remove ETFs
screen = screen[~screen["Company Name"].str.contains("Lyxor Smart Overnight Return - IE")]
screen = screen[screen["ETP Basket header date"].isna()]
screen = screen.drop("ETP Basket header date", axis=1)  # remove an empty column
screen = screen[screen["RIC"].notna()]  # remove empty rows
screen = screen.drop("Unnamed: 6", axis=1)  # same as ISIN
screen = screen.drop("Identifier", axis=1)  # same as RIC
screen.to_csv("./files/screen_230706.csv", index=False)

# prepare for merging listed and delisted
listed = pd.read_csv('./files/screen_230706.csv')[['Company Name', 'RIC']]
delisted = pd.read_csv("./files/delisted.csv")[[
    'Name (or Code)', 'RIC', 'RIC1(ticker)', 'RIC2(exchange)', 'delisted mm', 'delisted yy']]
delisted = delisted[delisted['RIC2(exchange)'] == 'L']  # Strictly LSE. Not 'Lp' (probably preferred, and 'TRE' (idk)
delisted = delisted.drop('RIC2(exchange)', axis=1)  # Now that all are 'L' type, no need

listed['RIC1(ticker)'] = listed['RIC'].str.replace('.L', '')
listed['delisted MM'] = 0  # not delisted
listed['delisted YY'] = 0  # not delisted

delisted = delisted.rename(
    columns={'Name (or Code)': 'Company Name', 'delisted mm': 'delisted MM', 'delisted yy': 'delisted YY'})

# merging two dataframes
all_firms = pd.concat([listed, delisted], axis=0)

comp_list = all_firms["RIC"].to_list()
total_length = len(comp_list)
isins = [None for _ in range(total_length)]
cusips = [None for _ in range(total_length)]
sedols = [None for _ in range(total_length)]
# ipodates = [None for _ in range(total_length)]
divide_by = 5000
already_have_until = 0
if already_have_until > 0:
    df_already_have = pd.read_csv('./files/comp_list_all.csv')
    isins = df_already_have["ISIN"].tolist()
    cusips = df_already_have["CUSIP"].tolist()
    sedols = df_already_have["SEDOL"].tolist()
    # ipodates = df_already_have["IPO Date"].tolist()

for i in range(math.ceil((total_length - already_have_until) // divide_by)):
    smaller_list = comp_list[already_have_until + i * divide_by: already_have_until + i * divide_by + divide_by]
    companies_db = dbb.Companies(smaller_list)

    isins[already_have_until + i * divide_by: already_have_until + i * divide_by + divide_by] = companies_db.isins
    cusips[already_have_until + i * divide_by: already_have_until + i * divide_by + divide_by] = companies_db.cusips
    sedols[already_have_until + i * divide_by: already_have_until + i * divide_by + divide_by] = companies_db.sedols
    # ipodates[already_have_until + i * divide_by: already_have_until + i * divide_by + divide_by] = companies_db.ipodates

    all_firms["ISIN"] = isins
    all_firms["CUSIP"] = cusips
    all_firms["SEDOL"] = sedols
    # all_firms["IPO Dates"] = ipodates

    all_firms.to_csv('./files/comp_list_all.csv', index=False)

duplicates = all_firms[all_firms.duplicated(subset=["Company Name"], keep=False)]
duplicates = duplicates.sort_values(by="RIC1(ticker)")
duplicates.to_csv('./files/comp_list_dups.csv', index=False)

no_duplicates = all_firms.drop_duplicates(subset=["Company Name"], keep="last")
no_duplicates.to_csv('./files/comp_list.csv', index=False)
