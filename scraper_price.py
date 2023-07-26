import myEikon as mek
import pandas as pd
import time
import os
import platform

'''
1. fetch ohlcv data from eikon and save as {ric1(ticker)}.csv at /price_data/
2. merge to {one_of_ohlcv}.csv at /price_data_merged/
3. convert back to {ric1(ticker)}.csv at /price_data_fixed/
'''


def fix_ohlccv(ohlccv_df: pd.DataFrame):
    """
    Fill in the blanks of ``ohlccv_df`` and returns the fixed dataframe
    :param ohlccv_df: the dataframe to fix
    :return: pd.DataFrame of the fixed price data
    """
    if len(ohlccv_df) < 1:
        print(f'!!!!!!!!!!!!!!!!{afile} is empty!!!!!!!!!!!!!!!!!')
        return None
    if 'HIGH' not in ohlccv_df.columns:
        ohlccv_df['HIGH'] = ohlccv_df['CLOSE']
    if 'LOW' not in ohlccv_df.columns:
        ohlccv_df['LOW'] = ohlccv_df['CLOSE']
    if 'OPEN' not in ohlccv_df.columns:
        ohlccv_df['OPEN'] = ohlccv_df['CLOSE']
    if 'COUNT' not in ohlccv_df.columns:
        ohlccv_df['COUNT'] = 0
    if 'VOLUME' not in ohlccv_df.columns:
        ohlccv_df['VOLUME'] = 0
    ohlc_df = ohlccv_df[['Date', 'HIGH', 'LOW', 'CLOSE', 'OPEN']]
    # shrout_df = ohlccv_df[['Date', 'SHROUT']]
    cv_df = ohlccv_df[['Date', 'COUNT', 'VOLUME']].fillna(0)
    nan_counter = ohlc_df.isna().sum(axis=1)
    for i, count in enumerate(nan_counter):
        if count < 1:
            continue
        # Only one datum among ohlc is missing -> high=max, low=min, close/open=some good num
        elif count == 1:
            if pd.isna(ohlccv_df.iloc[i]['HIGH']):
                ohlc_df.at[i, 'HIGH'] = max(ohlc_df.at[i, 'OPEN'], ohlc_df.at[i, 'CLOSE'])
            elif pd.isna(ohlccv_df.iloc[i]['LOW']):
                ohlc_df.at[i, 'LOW'] = min(ohlc_df.at[i, 'OPEN'], ohlc_df.at[i, 'CLOSE'])
            elif pd.isna(ohlccv_df.iloc[i]['CLOSE']):
                ohlc_df.at[i, 'CLOSE'] = ohlc_df.at[i, 'HIGH'] + ohlc_df.at[i, 'LOW'] - ohlc_df.at[
                    i, 'OPEN']
            else:
                ohlc_df.at[i, 'OPEN'] = ohlc_df.at[i, 'HIGH'] + ohlc_df.at[i, 'LOW'] - ohlc_df.at[
                    i, 'CLOSE']
        # two data are missing
        elif count == 2:
            num1, num2 = ohlc_df.loc[i].dropna().tolist()[1:]
            ohlc_df.at[i, 'HIGH'] = max(num1, num2)
            ohlc_df.at[i, 'LOW'] = min(num1, num2)
            ohlc_df.at[i, 'CLOSE'] = num1
            ohlc_df.at[i, 'OPEN'] = num2
        # only one is not a nan
        elif count == 3:
            date, the_only_value = ohlc_df.iloc[i].dropna().tolist()
            ohlc_df.loc[i] = the_only_value
            ohlc_df.at[i, 'Date'] = date
        # all are missing -> fill last close value
        else:
            date = ohlc_df.at[i, 'Date']
            if i == 0:
                the_price = ohlc_df.at[i + 1, 'OPEN']
            else:
                the_price = ohlc_df.at[i - 1, 'CLOSE']
            ohlc_df.loc[i] = the_price
            ohlc_df.at[i, 'Date'] = date
    ohlc_df.loc[:, 'Date'] = mek.datetime_to_str(ohlc_df.loc[:, 'Date'])
    cv_df.loc[:, 'Date'] = mek.datetime_to_str(cv_df.loc[:, 'Date'])
    # ohlccv_df_c = pd.concat([ohlccv_df, cv_df], axis=1)
    ohlccv_df = ohlc_df.merge(cv_df, on="Date", how="outer")

    for i in range(1, len(ohlccv_df)):
        if ohlccv_df.at[i, 'VOLUME'] == 0 and ohlccv_df.at[i, 'CLOSE'] != ohlccv_df.at[i, 'OPEN']:
            ohlccv_df.at[i, 'VOLUME'] = 1
        if i > 0 and ohlccv_df.at[i, 'VOLUME'] == 0 and ohlccv_df.at[i - 1, "CLOSE"] != ohlccv_df.at[i, "CLOSE"]:
            ohlccv_df.at[i, 'VOLUME'] = 1
    return ohlccv_df


def fix_ohlccv_file(input_file, output_file):
    """
    Fix ohlccv dataframe of the ``input_file`` and export as ``output_file``
    :param input_file: name of the file to fix. should end with .csv
    :param output_file: name of the fixed file. should end with .csv
    :return: None if the file is empty. Else, return the fixed dataframe as pandas.DataFrame
    """
    ohlccv_df = pd.read_csv(input_file)
    ohlccv_df = fix_ohlccv(ohlccv_df)
    if ohlccv_df is not None:
        ohlccv_df.to_csv(output_file, index=False)
    return ohlccv_df


adj = True
price_dir = 'files/price_stuff/adj_price_data/' if adj else 'files/price_stuff/price_data/'
fixed_price_dir = 'files/price_stuff/adj_price_data_fixed/' if adj else 'files/price_stuff/price_data_fixed/'
merge_dir = 'files/price_stuff/adj_price_data_merged/' if adj else 'files/price_stuff/price_data_merged/'
adj_filename = 'adj_' if adj else ''

if __name__ == '__main__':
    # b/c I have a Windows pc for fetching and a Mac for cleaning up
    fetchQ, shroutQ, fixQ, mergeQ, fillQ, convertQ = False, False, True, True, True, True
    if platform.system() != 'Darwin':
        fetchQ, shroutQ, fixQ, mergeQ, fillQ, convertQ = False, True, False, False, False, False

    # fetching data using eikon data api and save as {ric1(ticker)}.csv in /price_data/
    fetchQ = fetchQ
    if fetchQ:
        # choosing which pickle file to fetch rics from
        use_available = True
        if use_available:
            comp_list_df = pd.read_pickle('files/comp_list/available.pickle')
            rics = comp_list_df['RIC']
        else:
            comp_list_df = pd.read_pickle('files/comp_list/comp_list.pickle')
            rics = comp_list_df["RIC"]

            with open('files/comp_list/no_data.txt', 'r') as f:
                emptylist = [line.rstrip() for line in f]
            with open('files/comp_list/no_timestamp.txt', 'r') as f:
                notimestamplist = [line.rstrip() for line in f]
            rics = rics[~rics.isin(emptylist) * ~rics.isin(notimestamplist)]

        already_done = [file_name.split('.')[0].replace('-', '.') for file_name in os.listdir(price_dir)]
        rics = rics[~rics.isin(already_done)]

        for ric in rics:
            delistedYY = comp_list_df[comp_list_df['RIC'] == ric]['delisted YY'].sum()
            delistedYY = int(delistedYY)
            is_listed = delistedYY == 0
            try:
                comp = mek.Company(ric)
                if is_listed:
                    price_data = comp.fetch_price(adj=adj)
                    time.sleep(1)
                else:
                    print(f'{ric} is delisted in {delistedYY}..')
                    price_data = comp.fetch_price(delisted=delistedYY, adj=adj)
                    time.sleep(1)
                if len(price_data) > 0:
                    price_data.to_csv(price_dir + ric.replace('.', '-') + '.csv')
                    print(f"{ric} completed!\n")
                else:
                    print(f"!!!!!!!!!!!!!!!!!!!!!!!!!!!!!Empty Dataset for {ric}"
                          f"!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n")
                    emptylist.append(ric)
                    with open('files/comp_list/no_data.txt', 'w') as f:
                        for line in emptylist:
                            f.write(f"{line}\n")
                # time.sleep(1)
            except ValueError:
                print(f'ValueError: \'TIMESTAMP\' is not in list for {ric}\n')
                notimestamplist.append(ric)
                with open('files/comp_list/no_timestamp.txt', 'w') as f:
                    for line in notimestamplist:
                        f.write(f"{line}\n")

        # updating /files/comp_list/available.csv
        if not use_available:
            avail_list = [file_name[:-4].replace('-', '.') for file_name in os.listdir(price_dir)]
            avail_df = comp_list_df[comp_list_df['RIC'].isin(avail_list)]
            avail_df.to_csv('files/comp_list/available.csv', index=False)
            avail_df.to_pickle('files/comp_list/available.pickle')

    shroutQ = shroutQ
    if shroutQ:
        overwrite = False
        keep_both = False
        keep_orig = False
        file_list = os.listdir('files/price_stuff/price_data/')
        rics = [file_name[:-4].replace('-', '.') for file_name in file_list]
        for ric in rics:
            original_df = pd.read_csv(f'files/price_stuff/price_data/{ric.replace(".", "-")}.csv')

            # duplicate columns other than 'datadate' might exist so resolve this
            if ('SHROUT' in original_df.columns) and (not overwrite) and (not keep_both) and (not keep_orig):
                action = input(f'Which action would you like to perform? [overwrite/keep_both/keep_original] ')
                while action not in ['overwrite', 'keep_both', 'keep_original']:
                    action = input(f'Which action would you like to perform? [overwrite/keep_both/keep_original] ')

                if action == 'overwrite':
                    overwrite = True
                elif action == 'keep_both':
                    keep_both = True
                else:
                    keep_orig = True

                remem = input('Remember your choice? [y/n] ')
                while remem not in ['y', 'n']:
                    remem = input('Remember your choice? [y/n] ')
                if remem == 'n':
                    overwrite, keep_both, keep_orig = False, False, False

            if keep_orig and ('SHROUT' in original_df.columns):
                continue  # no need to fetch new

            original_df = original_df.drop('SHROUT', axis=1) \
                if (overwrite and 'SHROUT' in original_df.columns) else original_df

            comp = mek.Company(ric)
            shrout_df = pd.DataFrame()
            syear = int(original_df['Date'].min()[:4])
            eyear = int(original_df['Date'].max()[:4])
            eyear = eyear if eyear > syear else eyear+1
            for yyyy in range(syear, eyear, 10):
                to_concat = comp.fetch_shrout(start=str(yyyy)+'-01-01', end=str(yyyy+9)+'-12-31')
                shrout_df = pd.concat([shrout_df, to_concat])

            if shrout_df.isna().all().all():
                original_df['SHROUT'] = float('NaN')
                original_df.to_csv(f'files/price_stuff/price_data/{ric.replace(".", "-")}.csv', index=False)
                print(f'{ric} does not have shrout data!')
                continue
            original_df = original_df.merge(shrout_df, on='Date', how='outer').sort_values(by='Date')
            original_df = original_df.dropna(subset=['HIGH', 'LOW', 'CLOSE', 'OPEN', 'VOLUME'], how='all')
            original_df.to_csv(f'files/price_stuff/price_data/{ric.replace(".", "-")}.csv', index=False)
            print(f'{ric} shrout done!')

    # wisely fill NaNs and save at /price_data/
    fixQ = fixQ
    retryQ = False
    if fixQ:
        files = os.listdir(price_dir)
        finished_ones = os.listdir(fixed_price_dir)
        finished_count = 0
        for afile in files:
            if retryQ or afile not in finished_ones:
                cont_if_none = fix_ohlccv_file(price_dir + afile, fixed_price_dir + afile)
                if cont_if_none is None:
                    continue
            finished_count += 1
            print(f'{finished_count}/{len(files)} | {afile} done!')

    # merging data to make {one_of_ohlcv}.csv at /price_data_merged/
    mergeQ = mergeQ
    if mergeQ:
        files = os.listdir(fixed_price_dir)
        df = pd.read_csv(fixed_price_dir + files[0])
        firm_name = files[0].split('.')[0]
        phigh = df[['Date', 'HIGH']].rename(columns={'HIGH': firm_name})
        plow = df[['Date', 'LOW']].rename(columns={'LOW': firm_name})
        popen = df[['Date', 'OPEN']].rename(columns={'OPEN': firm_name})
        pclose = df[['Date', 'CLOSE']].rename(columns={'CLOSE': firm_name})

        print('\nMerging price data...\n[', end="")
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

        phigh.to_csv(merge_dir + adj_filename + 'high.csv', index=True)
        plow.to_csv(merge_dir + adj_filename + 'low.csv', index=True)
        popen.to_csv(merge_dir + adj_filename + 'open.csv', index=True)
        pclose.to_csv(merge_dir + adj_filename + 'close.csv', index=True)
        print('Finished merging price data!')

    # fill in the blanks of the merged data with prev values and save at /price_data_merged/
    fillQ = fillQ
    if fillQ:
        merged_files = os.listdir(merge_dir)
        print('\nRefilling empty price values...')
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

            df = df.reindex(sorted(df.columns), axis=1)
            df.to_csv(merge_dir + afile, index=False)
            print(f'{adj_filename}{afile} finished!')

    # convert fixed date vs comp matrix into date vs ohlc matrix and save as {ric1(ticker)}.csv at /price_data_fixed/
    convertQ = convertQ
    if convertQ:
        df_c = pd.read_csv(merge_dir + adj_filename + 'close.csv')
        df_h = pd.read_csv(merge_dir + adj_filename + 'high.csv')
        df_l = pd.read_csv(merge_dir + adj_filename + 'low.csv')
        df_o = pd.read_csv(merge_dir + adj_filename + 'open.csv')
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
