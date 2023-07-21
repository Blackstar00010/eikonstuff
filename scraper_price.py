import myEikon as dbb
import pandas as pd
import numpy as np
from eikon import eikonError
import time
import os

'''
1. fetch ohlcv data from eikon and save as {ric1(ticker)}.csv at /price_data/
2. merge to {one_of_ohlcv}.csv at /price_data_merged/
3. convert back to {ric1(ticker)}.csv at /price_data_fixed/
'''

price_dir = 'files/price_stuff/price_data/'
fixed_price_dir = 'files/price_stuff/price_data_fixed/'
merge_dir = 'files/price_stuff/price_data_merged/'

# fetching data using eikon data api and save as {ric1(ticker)}.csv at /price_data/
fetchQ = False
if fetchQ:
    comp_list = pd.read_pickle('files/comp_list/comp_list.pickle')
    rics = comp_list["RIC"]

    dup_ric_list = comp_list[comp_list.duplicated(subset='RIC1(ticker)', keep=False)][['Company Name', 'RIC',
                                                                                       'RIC1(ticker)', 'ISIN']]
    dup_ric_list = dup_ric_list.sort_values(by='RIC1(ticker)')
    dup_ric_list.to_csv('files/comp_list/comp_list_dup_ric1.csv')

    already_done = [i.split('.')[0] for i in os.listdir(price_dir)]
    with open('files/comp_list/no_data.txt', 'r') as f:
        emptylist = [line.rstrip() for line in f]
    with open('files/comp_list/no_timestamp.txt', 'r') as f:
        notimestamplist = [line.rstrip() for line in f]
    # mask = [(ric.replace('.', '-') not in already_done and
    #          ric not in emptylist and
    #          ric not in notimestamplist)
    #         for ric in rics]
    rics = rics[rics.isin(dup_ric_list['RIC']) * ~rics.isin(emptylist) * ~rics.isin(notimestamplist)]

    for ric in rics:
        endyear = comp_list[comp_list['RIC'] == ric]['delisted YY'].sum()
        try:
            comp = dbb.Company(ric)
            if endyear == 0:
                price_data = comp.fetch_price()
            else:
                print(f'{ric} is delisted in {endyear}..')
                price_data = comp.fetch_price(delisted=endyear, adj=False)
            if len(price_data) > 0:
                price_data.to_csv(price_dir + ric.replace('.', '-') + '.csv')
                print(f"{ric} completed!")
            else:
                print(f"!!!!!!!!!!!!!!!!!!!!!!!!!!!!!Empty Dataset for {ric}!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
                emptylist.append(ric)
                with open('files/comp_list/no_data.txt', 'w') as f:
                    for line in emptylist:
                        f.write(f"{line}\n")
            time.sleep(1)
        except ValueError:
            print(f'ValueError: \'TIMESTAMP\' is not in list for {ric}')
            notimestamplist.append(ric)
            with open('files/comp_list/no_timestamp.txt', 'w') as f:
                for line in notimestamplist:
                    f.write(f"{line}\n")

# wisely fill NaNs and save at /price_data/
fixQ = True
retryQ = True
if fixQ:
    files = os.listdir(price_dir)
    finished_ones = os.listdir(fixed_price_dir)
    finished_count = 0
    for afile in files:
        if retryQ or afile not in finished_ones:
            df = pd.read_csv(price_dir + afile)
            if len(df) < 1:
                print(f'!!!!!!!!!!!!!!!!{afile} is empty!!!!!!!!!!!!!!!!!')
                continue
            if 'HIGH' not in df.columns:
                df['HIGH'] = df['CLOSE']
            if 'LOW' not in df.columns:
                df['LOW'] = df['CLOSE']
            if 'OPEN' not in df.columns:
                df['OPEN'] = df['CLOSE']
            if 'COUNT' not in df.columns:
                df['COUNT'] = 0
            if 'VOLUME' not in df.columns:
                df['VOLUME'] = 0
            hlco_df = df[['Date', 'HIGH', 'LOW', 'CLOSE', 'OPEN']]
            # shrout_df = df[['Date', 'SHROUT']]
            cv_df = df[['Date', 'COUNT', 'VOLUME']].fillna(0)
            if hlco_df.isna().any().any():
                nan_counter = hlco_df.isna().sum(axis=1)
                for i, count in enumerate(nan_counter):
                    # Only one datum among ohlc is missing -> high=max, low=min, close/open=some good num
                    if count == 1:
                        if pd.isna(df.iloc[i]['HIGH']):
                            hlco_df.at[i, 'HIGH'] = max(hlco_df.at[i, 'OPEN'], hlco_df.at[i, 'CLOSE'])
                        elif pd.isna(df.iloc[i]['LOW']):
                            hlco_df.at[i, 'LOW'] = min(hlco_df.at[i, 'OPEN'], hlco_df.at[i, 'CLOSE'])
                        elif pd.isna(df.iloc[i]['CLOSE']):
                            hlco_df.at[i, 'CLOSE'] = hlco_df.at[i, 'HIGH'] + hlco_df.at[i, 'LOW'] - hlco_df.at[
                                i, 'OPEN']
                        else:
                            hlco_df.at[i, 'OPEN'] = hlco_df.at[i, 'HIGH'] + hlco_df.at[i, 'LOW'] - hlco_df.at[
                                i, 'CLOSE']
                    # two data are missing
                    elif count == 2:
                        num1, num2 = hlco_df.loc[i].dropna().tolist()[1:]
                        hlco_df.at[i, 'HIGH'] = max(num1, num2)
                        hlco_df.at[i, 'LOW'] = min(num1, num2)
                        hlco_df.at[i, 'CLOSE'] = num1
                        hlco_df.at[i, 'OPEN'] = num2
                    # only one is not a nan
                    elif count == 3:
                        date, the_only_value = hlco_df.iloc[i].dropna().tolist()
                        hlco_df.loc[i] = the_only_value
                        hlco_df.at[i, 'Date'] = date
                    # all are missing -> fill last close value
                    else:
                        date = hlco_df.at[i, 'Date']
                        if i == 0:
                            the_price = hlco_df.at[i + 1, 'OPEN']
                        else:
                            the_price = hlco_df.at[i - 1, 'CLOSE']
                        hlco_df.loc[i] = the_price
                        hlco_df.at[i, 'Date'] = date
            df = hlco_df.merge(cv_df, on="Date", how="outer")

            for i in range(1, len(df)):
                if df.at[i, 'VOLUME'] == 0 and df.at[i, 'CLOSE'] != df.at[i, 'OPEN']:
                    df.at[i, 'VOLUME'] = 1
                if i > 0 and df.at[i, 'VOLUME'] == 0 and df.at[i - 1, "CLOSE"] != df.at[i, "CLOSE"]:
                    df.at[i, 'VOLUME'] = 1
            df.to_csv(fixed_price_dir + afile, index=False)
        finished_count += 1
        print(f'{finished_count}/{len(files)} | {afile} done!')

# merging data to make {one_of_ohlcv}.csv at /price_data_merged/
mergeQ = True
if mergeQ:
    files = os.listdir(fixed_price_dir)
    df = pd.read_csv(fixed_price_dir + files[0])
    firm_name = files[0].split('.')[0]
    phigh = df[['Date', 'HIGH']].rename(columns={'HIGH': firm_name})
    plow = df[['Date', 'LOW']].rename(columns={'LOW': firm_name})
    popen = df[['Date', 'OPEN']].rename(columns={'OPEN': firm_name})
    pclose = df[['Date', 'CLOSE']].rename(columns={'CLOSE': firm_name})

    print('Merging price data...\n[', end="")
    for i, afile in enumerate(files[1:]):
        df = pd.read_csv(fixed_price_dir + afile)
        firm_name = afile.split('.')[0]
        phigh = phigh.merge(df[['Date', 'HIGH']], on="Date", how="outer").rename(columns={'HIGH': firm_name})
        plow = plow.merge(df[['Date', 'LOW']], on="Date", how="outer").rename(columns={'LOW': firm_name})
        popen = popen.merge(df[['Date', 'OPEN']], on="Date", how="outer").rename(columns={'OPEN': firm_name})
        pclose = pclose.merge(df[['Date', 'CLOSE']], on="Date", how="outer").rename(columns={'CLOSE': firm_name})

        if i % 5 == 3:
            print('-', end="")
        if i % 100 == 98:
            print(f']  {i + 2}/{len(files)}\n[', end="")
    print(f']  {len(files)}/{len(files)}')

    phigh = phigh.sort_values('Date').set_index('Date')
    plow = plow.sort_values('Date').set_index('Date')
    popen = popen.sort_values('Date').set_index('Date')
    pclose = pclose.sort_values('Date').set_index('Date')

    phigh.to_csv(merge_dir + 'high.csv', index=True)
    plow.to_csv(merge_dir + 'low.csv', index=True)
    popen.to_csv(merge_dir + 'open.csv', index=True)
    pclose.to_csv(merge_dir + 'close.csv', index=True)
    print('Finished merging price data!')

# fill in the blanks of the merged data with prev values and save at /price_data_merged/
fillQ = True
if fillQ:
    merged_files = os.listdir(merge_dir)
    print('Refilling empty price values...')
    for afile in merged_files:
        df = pd.read_csv(merge_dir + afile)
        first_i = df.apply(lambda col: col.first_valid_index())
        last_i = df.apply(lambda col: col.last_valid_index())
        for acol in df.columns:
            prev_price = 0
            for i in range(first_i[acol], last_i[acol]):
                if pd.isna(df.at[i, acol]):
                    df.at[i, acol] = prev_price
                prev_price = df.at[i, acol]
        df.to_csv(merge_dir + afile, index=False)
        print(f'{afile} finished!')

# convert fixed date vs comp matrix into date vs ohlc matrix and save as {ric1(ticker)}.csv at /price_data_fixed/
convertQ = True
if convertQ:
    df_c = pd.read_csv(merge_dir + 'close.csv')
    df_h = pd.read_csv(merge_dir + 'high.csv')
    df_l = pd.read_csv(merge_dir + 'low.csv')
    df_o = pd.read_csv(merge_dir + 'open.csv')
    files = os.listdir(fixed_price_dir)

    print('Converting back...\n[', end="")
    for i, afile in enumerate(files[27:]):
        firm_name = afile.split('.')[0]
        old_df = pd.read_csv(fixed_price_dir + afile)[['Date', 'COUNT', 'VOLUME']]
        new_df = pd.DataFrame()
        new_df['Date'] = df_c['Date']
        new_df['CLOSE'] = df_c[firm_name]
        new_df['HIGH'] = df_h[firm_name]
        new_df['LOW'] = df_l[firm_name]
        new_df['OPEN'] = df_o[firm_name]
        new_df = new_df.dropna()
        new_df = new_df.merge(old_df, on="Date", how="outer").fillna(0)
        new_df.to_csv(fixed_price_dir + afile, index=False)

        if i % 5 == 4:
            print('-', end="")
        if i % 100 == 99:
            print(f'] {i + 1}/{len(files)}\n[', end="")
    print(f'] {len(files)}/{len(files)}')
