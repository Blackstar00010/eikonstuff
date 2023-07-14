import pandas as pd

'''
files/comp_list/comp_list.csv (from Refinitiv) + ~/comp_names_all.csv (from Compustat) -> files/tl_table/ref-comp.csv
'''

dt_wrds_dir = '/Users/jamesd/Desktop/data/all/'

# TODO: After fetching all prices, we should check if two companies with different RIC but same RIC(ticker) exists
refi_df = pd.read_csv('files/comp_list/comp_list.csv')[['Company Name', 'RIC', 'RIC1(ticker)', 'ISIN', 'CUSIP']]
refi_df = refi_df[refi_df['ISIN'].notna()]
refi_df = refi_df.rename(columns=str.lower)
comp_df = pd.read_csv(dt_wrds_dir+'comp_names_all.csv')[['gvkey', 'isin']]
comp_df = comp_df[comp_df['isin'].notna()]

# merging side by side
# -> columns: ['company name', 'ric', 'ric1(ticker)', 'isin', 'gvkey', 'conm', 'cusip']
merged_df = pd.merge(refi_df, comp_df, on='isin', how='inner')

# resolving duplicated rows problem
dup_isins = merged_df[merged_df['isin'].duplicated()]['isin']
print('# of duplicate rows:', len(dup_isins))  # TODO: debug here
for isin in dup_isins:
    shits = merged_df[merged_df['isin'].isin(dup_isins)].sort_values('isin')['ric'].tolist()
    shits_df = pd.DataFrame()
    for ric in shits:
        ric = ric.replace('.', '-')
        # shit_df = pd.read_csv('')  # TODO: where I store the price data
        # shits_df = pd.concat([shits_df, shit_df], axis=0)
        # shit_df.to_csv('')  # TODO: The same directory, but something like "JPM-L-dup.csv"
    # TODO: Somehow remove duplicate dates' prices

merged_df = merged_df[~merged_df['isin'].duplicated()]

# columns: ['company name', 'ric', 'ric1(ticker)', 'isin', 'cusip', 'gvkey']
merged_df.to_csv('files/tl_table/ref-comp.csv', index=False)
print('# of unique translatable companies:', len(merged_df))
