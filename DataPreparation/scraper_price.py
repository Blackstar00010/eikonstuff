import pandas as pd
from Misc import useful_stuff
import shutil

'''
1. fetch ohlcv data from eikon and save as {ric1(ticker)}.csv at /price_data/
2. merge to {one_of_ohlcv}.csv at /price_data_merged/
3. convert back to {ric1(ticker)}.csv at /price_data_fixed/
'''


def fix_ohlccv(ohlccv_df: pd.DataFrame) -> pd.DataFrame:
    """
    Fill in the blanks of ``ohlccv_df`` and returns the fixed dataframe
    :param ohlccv_df: the dataframe to fix
    :return: pd.DataFrame of the fixed price data
    """
    if len(ohlccv_df) < 1:
        print(f'!!!!!!!!!!!!!!!!{afile} is empty!!!!!!!!!!!!!!!!!')
        return pd.DataFrame()
    for data_type in ['HIGH', 'LOW', 'OPEN']:
        if data_type not in ohlccv_df.columns:
            ohlccv_df[data_type] = ohlccv_df['CLOSE']
    for data_type in ['COUNT', 'VOLUME']:
        if data_type not in ohlccv_df.columns:
            ohlccv_df[data_type] = 0
    if 'Date' not in ohlccv_df.columns:
        ohlccv_df = ohlccv_df.reset_index(drop=False).rename(columns={'index': 'Date'})

    ohlccv_df = ohlccv_df.dropna(subset='Date')

    compute_shrout = 'SHROUT' in ohlccv_df.columns
    if compute_shrout:
        shrout_df = ohlccv_df[['Date', 'SHROUT']]
        shrout_df.loc[:, 'SHROUT'] = shrout_df.loc[:, 'SHROUT'].fillna(method='ffill')

    ohlc_df = ohlccv_df[['Date', 'HIGH', 'LOW', 'CLOSE', 'OPEN']]
    cv_df = ohlccv_df[['Date', 'COUNT', 'VOLUME']].fillna(0)

    nan_counter = ohlc_df.isna().sum(axis=1)
    for i, count in enumerate(nan_counter):
        if count < 1:  # no empty
            continue

        # Only one datum among ohlc is missing -> high=max, low=min, close/open=some good no.
        elif count == 1:
            if pd.isna(ohlc_df.iloc[i]['HIGH']):  # todo: iloc[i]['HIGH'] -> loc[i, 'HIGH']
                ohlc_df.at[i, 'HIGH'] = max(ohlc_df.at[i, 'OPEN'], ohlc_df.at[i, 'CLOSE'])
            elif pd.isna(ohlc_df.iloc[i]['LOW']):
                ohlc_df.at[i, 'LOW'] = min(ohlc_df.at[i, 'OPEN'], ohlc_df.at[i, 'CLOSE'])
            elif pd.isna(ohlc_df.iloc[i]['CLOSE']):
                ohlc_df.at[i, 'CLOSE'] = ohlc_df.at[i, 'HIGH'] + ohlc_df.at[i, 'LOW'] - ohlc_df.at[i, 'OPEN']
            else:
                ohlc_df.at[i, 'OPEN'] = ohlc_df.at[i, 'HIGH'] + ohlc_df.at[i, 'LOW'] - ohlc_df.at[i, 'CLOSE']

        # two data are missing -> high=max, low=min, close/open=some good no.
        elif count == 2:
            num1, num2 = ohlc_df.loc[i, :].dropna().tolist()[1:]
            ohlc_df.loc[i, ['HIGH', 'LOW', 'CLOSE', 'OPEN']] = [max(num1, num2), min(num1, num2), num1, num2]

        # only one is not a nan
        elif count == 3:
            date, the_only_value = ohlc_df.loc[i, :].dropna().tolist()
            ohlc_df.loc[i, ['HIGH', 'LOW', 'CLOSE', 'OPEN']] = the_only_value
            ohlc_df.at[i, 'Date'] = date

        else:  # all four missing
            if i > 0:
                prev_close = ohlc_df.loc[i - 1, 'CLOSE']
                ohlc_df.loc[i, ['HIGH', 'LOW', 'CLOSE', 'OPEN']] = prev_close
                if pd.isna(prev_close):
                    cv_df.loc[i, :] = prev_close

    ohlc_df.loc[:, 'Date'] = useful_stuff.datetime_to_str(ohlc_df.loc[:, 'Date'])
    cv_df.loc[:, 'Date'] = useful_stuff.datetime_to_str(cv_df.loc[:, 'Date'])
    ohlccv_df = ohlc_df.merge(cv_df, on="Date", how="outer")

    if compute_shrout:
        shrout_df.loc[:, 'Date'] = useful_stuff.datetime_to_str(shrout_df.loc[:, 'Date'])
        ohlccv_df = ohlccv_df.merge(shrout_df, on="Date", how="outer")

    for i in range(1, len(ohlccv_df)):
        if (ohlccv_df.at[i, 'VOLUME'] == 0) and (ohlccv_df.at[i, 'CLOSE'] != ohlccv_df.at[i, 'OPEN']):
            ohlccv_df.at[i, 'VOLUME'] = 1
        if (i > 0) and (ohlccv_df.at[i, 'VOLUME'] == 0) and (ohlccv_df.at[i - 1, "CLOSE"] != ohlccv_df.at[i, "CLOSE"]):
            ohlccv_df.at[i, 'VOLUME'] = 1
    return ohlccv_df


def fix_ohlccv_file(input_file: str, output_file: str, retryQ=True) -> pd.DataFrame:
    """
    Fix ohlccv dataframe of the ``input_file`` and export as ``output_file``
    :param input_file: name of the file to fix. should end with .csv
    :param output_file: name of the fixed file. should end with .csv
    :param retryQ: fix again even if the fixed file exists in the target directory
    :return: None if the file is empty. Else, return the fixed dataframe as pandas.DataFrame
    """
    # input_dir = '/'.join(input_file.split('/')[:-1])
    output_dir = '/'.join(output_file.split('/')[:-1])
    filename = input_file.split('/')[-1]

    # todo: remove retryQ
    finished_count = 0
    if retryQ or filename not in useful_stuff.listdir(output_dir):
        ohlccv_df = pd.read_csv(input_file)
        ohlccv_df = fix_ohlccv(ohlccv_df)
        if ohlccv_df is not None:
            ohlccv_df.to_csv(output_file, index=False)
            finished_count += 1
            print(f'{filename} done!')
        return ohlccv_df


adj = True
unadj_price_dir = '../data/price_stuff/price_data/'
price_dir = '../data/price_stuff/adj_price_data/' if adj else unadj_price_dir
fixed_price_dir = '../data/price_stuff/adj_price_data_fixed/' if adj else '../data/price_stuff/price_data_fixed/'
merge_dir = '../data/price_stuff/adj_price_data_merged/' if adj else '../data/price_stuff/price_data_merged/'
secd_ref_dir = '../data/processed/input_secd/'
comp_list_dir = '../data/metadata/comp_list/'
adj_filename = 'adj_' if adj else ''

if __name__ == '__main__':
    import time
    import os
    import platform
    import myEikon as mek
    import multiprocessing as mp

    # b/c I have a Windows pc for fetching and a Mac for cleaning up
    fetchQ, shroutQ, fixQ, mergeQ, moveQ, convertQ = False, False, False, False, True, False
    if platform.system() != 'Darwin':
        fetchQ, shroutQ, fixQ, mergeQ, moveQ, convertQ = True, False, False, False, False, False

    # fetching data using eikon data api and save as {ric1(ticker)}.csv in /price_data/
    fetchQ = fetchQ
    if fetchQ:
        # choosing which pickle file to fetch rics from
        use_available = True
        if use_available:
            comp_list_df = pd.read_pickle(comp_list_dir + 'available.pickle')
            rics = comp_list_df['RIC']
        else:
            comp_list_df = pd.read_pickle(comp_list_dir + 'comp_list.pickle')
            rics = comp_list_df["RIC"]

            with open(comp_list_dir + 'no_data.txt', 'r') as f:
                emptylist = [line.rstrip() for line in f]
            with open(comp_list_dir + 'no_timestamp.txt', 'r') as f:
                notimestamplist = [line.rstrip() for line in f]
            rics = rics[~rics.isin(emptylist) * ~rics.isin(notimestamplist)]

        already_done = [file_name.split('.')[0].replace('-', '.') for file_name in useful_stuff.listdir(price_dir)]
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
                    print(f"!!!!!!!!!!!!!!!!!!!!!!!!!!!!!Empty Dataset for {ric}!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n")
                    emptylist.append(ric)
                    with open(comp_list_dir + 'no_data.txt', 'w') as f:
                        for line in emptylist:
                            f.write(f"{line}\n")
                # time.sleep(1)
            except ValueError:
                print(f'ValueError: \'TIMESTAMP\' is not in list for {ric}\n')
                notimestamplist.append(ric)
                with open(comp_list_dir + 'no_timestamp.txt', 'w') as f:
                    for line in notimestamplist:
                        f.write(f"{line}\n")

        # updating /data/comp_list/available.csv
        if not use_available:
            avail_list = [file_name[:-4].replace('-', '.') for file_name in useful_stuff.listdir(price_dir)]
            avail_df = comp_list_df[comp_list_df['RIC'].isin(avail_list)]
            avail_df.to_csv(comp_list_dir + 'available.csv', index=False)
            avail_df.to_pickle(comp_list_dir + 'available.pickle')
        useful_stuff.beep()

    # appending shrout data to .csv data in /price_data/
    shroutQ = shroutQ
    if shroutQ:
        overwrite = False
        keep_both = False
        keep_orig = False
        file_list = useful_stuff.listdir(unadj_price_dir)
        rics = [file_name[:-4].replace('-', '.') for file_name in file_list]
        for ric in rics:
            original_df = pd.read_csv(f'{unadj_price_dir}/{ric.replace(".", "-")}.csv')

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
                print(f'{ric} already done!')
                continue  # no need to fetch new

            original_df = original_df.drop('SHROUT', axis=1) \
                if (overwrite and 'SHROUT' in original_df.columns) else original_df

            comp = mek.Company(ric)
            shrout_df = pd.DataFrame()
            syear = int(original_df['Date'].min()[:4])
            eyear = int(original_df['Date'].max()[:4])
            eyear = eyear if eyear > syear else eyear + 1
            for yyyy in range(syear, eyear, 10):
                to_concat = comp.fetch_shrout(start=str(yyyy) + '-01-01', end=str(yyyy + 9) + '-12-31')
                shrout_df = pd.concat([shrout_df, to_concat])

            if shrout_df.isna().all().all():
                original_df['SHROUT'] = float('NaN')
                original_df.to_csv(f'{unadj_price_dir}/{ric.replace(".", "-")}.csv', index=False)
                print(f'{ric} does not have shrout data!')
                continue
            original_df = original_df.merge(shrout_df, on='Date', how='outer').sort_values(by='Date')
            existing_col = [col for col in ['HIGH', 'LOW', 'CLOSE', 'OPEN', 'VOLUME'] if col in original_df.columns]
            original_df = original_df.dropna(subset=existing_col, how='all')
            original_df['SHROUT'] = original_df['SHROUT'].fillna(method='ffill')

            original_df.to_csv(f'{unadj_price_dir}/{ric.replace(".", "-")}.csv', index=False)
            print(f'{ric} shrout done!')
        useful_stuff.beep()

    # wisely fill NaNs and save at /price_data_fixed/
    fixQ = fixQ
    if fixQ:
        pre_fix = False  # mp screws up some of the rows
        if pre_fix:
            print('Finding zeros...')
            for afile in useful_stuff.listdir(price_dir):
                shutil.copyfile(price_dir + afile, fixed_price_dir + afile)

            files = useful_stuff.listdir(fixed_price_dir)
            for afile in files:
                df = pd.read_csv(fixed_price_dir + afile)
                try:
                    useful_stuff.zero_finder(df, afile, raise_error=True)  # no error unless 0 exists
                except ValueError as e:
                    print(e.args[0])
                    df = df.replace(0, float('NaN'))
                    df = df.dropna(subset=['HIGH', 'LOW', 'CLOSE', 'OPEN', 'VOLUME'], how='all')

                df = useful_stuff.strip_df(df, subset=['HIGH', 'LOW', 'CLOSE', 'OPEN'], axis='row', method='first')
                if len(df) > 0:
                    df.to_csv(fixed_price_dir + afile, index=False)
                else:
                    os.remove(fixed_price_dir + afile)
            print('Finished handling zeros!')

            print('Dropping invalid data...')
            files = useful_stuff.listdir(fixed_price_dir)
            del_df = pd.read_pickle(comp_list_dir + 'comp_list.pickle')[['RIC', 'delisted MM', 'delisted YY']]
            for afile in files:
                ticker = afile[:-4]
                if '^' not in ticker:
                    continue
                ticker = ticker.replace('-', '.')
                del_mm, del_yy = del_df[del_df['RIC'] == ticker].values[0][1:]
                df = pd.read_csv(fixed_price_dir + afile)

                is_good_yy = pd.to_datetime(df['Date']).dt.strftime('%Y').astype(int) <= int(del_yy)
                if (~is_good_yy).any():
                    df = df[is_good_yy]

                is_bad_mm = (pd.to_datetime(df['Date']).dt.strftime('%m').astype(int) == (int(del_mm) + 1)) * \
                             (pd.to_datetime(df['Date']).dt.strftime('%Y').astype(int) == int(del_yy))
                is_bad_mm = is_bad_mm.replace(False, float('NaN')).fillna(method='ffill').replace(float('NaN'), False)
                if is_bad_mm.any():
                    df = df[~is_bad_mm]

                if len(df) > 0:
                    df.to_csv(fixed_price_dir + afile, index=False)
                else:
                    os.remove(fixed_price_dir + afile)
            print('Finished dropping invalid data!')

            print('Applying the same column/date format...', time.strftime('%H:%M'))
            # same column same date
            useful_stuff.give_same_format(fixed_price_dir, strip_rows=True,
                                          strip_rows_subset=['OPEN', 'HIGH', 'LOW', 'CLOSE'], print_logs=True)
            print('Finished applying the same formats')

        else:  # not pre_fix
            nthread = mp.cpu_count()
            files = useful_stuff.listdir(fixed_price_dir)
            print('Filling ohlccv file...', time.strftime('%H:%M'))
            for i in range(0, len(files), nthread):
                files_splitted = files[i:i + nthread]
                procs = []
                for afile in files_splitted:
                    proc = mp.Process(target=fix_ohlccv_file,
                                      args=(fixed_price_dir + afile, fixed_price_dir + afile, ))
                    procs.append(proc)
                    proc.start()
                for proc in procs:
                    proc.join()
        useful_stuff.beep()

    # merging data to make {one_of_ohlcvs}.csv at /price_data_merged/
    mergeQ = mergeQ
    if mergeQ:
        all_files = useful_stuff.listdir(fixed_price_dir)

        # split b/c merging a column is >O(n^2) (I think)
        split_by = 1000
        for i in range(0, len(all_files), split_by):
            files = all_files[i: i + split_by]
            df = pd.read_csv(fixed_price_dir + files[0])
            firm_name = files[0].split('.')[0].replace('-', '.')
            phigh = df[['Date', 'HIGH']].rename(columns={'HIGH': firm_name})
            plow = df[['Date', 'LOW']].rename(columns={'LOW': firm_name})
            popen = df[['Date', 'OPEN']].rename(columns={'OPEN': firm_name})
            pclose = df[['Date', 'CLOSE']].rename(columns={'CLOSE': firm_name})
            pvol = df[['Date', 'VOLUME']].rename(columns={'VOLUME': firm_name})
            if 'SHROUT' in df.columns:
                shrout = df[['Date', 'SHROUT']].rename(columns={'SHROUT': firm_name}) if not adj else 0
            else:
                shrout = pvol
                shrout[firm_name] = float('NaN')

            print('\nMerging price data...\n[', end="")
            for index, afile in enumerate(files[1:]):
                df = pd.read_csv(fixed_price_dir + afile)
                firm_name = afile.split('.')[0].replace('-', '.')
                phigh = phigh.merge(df[['Date', 'HIGH']], on="Date", how="outer").rename(columns={'HIGH': firm_name})
                plow = plow.merge(df[['Date', 'LOW']], on="Date", how="outer").rename(columns={'LOW': firm_name})
                popen = popen.merge(df[['Date', 'OPEN']], on="Date", how="outer").rename(columns={'OPEN': firm_name})
                pclose = pclose.merge(df[['Date', 'CLOSE']], on="Date", how="outer").rename(
                    columns={'CLOSE': firm_name})
                pvol = pvol.merge(df[['Date', 'VOLUME']], on="Date", how="outer").rename(columns={'VOLUME': firm_name})
                if 'SHROUT' in df.columns:
                    shrout = shrout.merge(df[['Date', 'SHROUT']], on="Date", how="outer"
                                          ).rename(columns={'SHROUT': firm_name}) if not adj else 0
                else:
                    to_merge = df[['Date', 'VOLUME']]
                    to_merge.loc[:, 'VOLUME'] = float('NaN')
                    shrout = shrout.merge(to_merge, on="Date", how="outer").rename(columns={'VOLUME': firm_name})

                if index % 5 == 3:
                    print('-', end="")
                if index % 100 == 98:
                    print(f']  {index + 2}/{len(files)}\n[', end="")
                if index == 200:
                    index = index  # debug point
            print(f']  {len(files)}/{len(files)}')

            print('Sorting and saving...')
            phigh = phigh.sort_values('Date').set_index('Date')
            plow = plow.sort_values('Date').set_index('Date')
            popen = popen.sort_values('Date').set_index('Date')
            pclose = pclose.sort_values('Date').set_index('Date')
            pvol = pvol.sort_values('Date').set_index('Date')
            shrout = shrout.sort_values('Date').set_index('Date') if not adj else 0

            phigh.to_csv(merge_dir + adj_filename + 'high' + str(i) + '.csv', index=True)
            plow.to_csv(merge_dir + adj_filename + 'low' + str(i) + '.csv', index=True)
            popen.to_csv(merge_dir + adj_filename + 'open' + str(i) + '.csv', index=True)
            pclose.to_csv(merge_dir + adj_filename + 'close' + str(i) + '.csv', index=True)
            pvol.to_csv(merge_dir + adj_filename + 'volume' + str(i) + '.csv', index=True)
            shrout.to_csv(merge_dir + adj_filename + 'shrout' + str(i) + '.csv', index=True) if not adj else 0

            print(f'Finished merging {i // split_by + 1} batch(es) out of {len(all_files) // split_by + 1} batch(es)!')

        ptype_list = ['high', 'low', 'open', 'close', 'volume'] if adj \
            else ['high', 'low', 'open', 'close', 'volume', 'shrout']

        for ptype in ptype_list:
            df = pd.read_csv(merge_dir + adj_filename + ptype + '0.csv')
            for i in range(split_by, len(all_files), split_by):
                df = df.merge(pd.read_csv(merge_dir + adj_filename + ptype + str(i) + '.csv'), on='Date')
            df.to_csv(merge_dir + adj_filename + ptype + '.csv', index=False)
            print(f'{ptype} done!')

        print('Finished merging price data!')

        useful_stuff.beep()
        if input('Do you wish to delete intermediary data? [y/n]') == 'y':
            for ptype in ['high', 'low', 'open', 'close', 'volume', 'shrout']:
                for i in range(split_by, len(all_files), split_by):
                    os.remove(merge_dir + adj_filename + ptype + str(i) + '.csv')
        useful_stuff.beep()

    # calculate mve based on close prices and move to /data/processed/input_secd
    moveQ = moveQ
    if moveQ:
        cols = pd.read_csv('../data/processed/input_funda/act.csv').columns  # RICs of companies that has fundamental data
        if adj:
            # moving to /data/processed/input_secd/
            close_adj_df = pd.read_csv(merge_dir + 'adj_close.csv').set_index('Date')
            close_adj_df = close_adj_df.loc[:, close_adj_df.columns[close_adj_df.columns.isin(cols)]]
            # error in TLW.L -> todo: edit those data and remerge
            close_adj_df = close_adj_df.replace(0, float('NaN')).fillna(method='ffill')
            close_adj_df.to_csv(secd_ref_dir + 'close_adj.csv')
        else:
            shrout_df = pd.read_csv(merge_dir + 'shrout.csv').set_index('Date')
            close_df = pd.read_csv(merge_dir + 'close.csv').set_index('Date')
            volume_df = pd.read_csv(merge_dir + 'volume.csv').set_index('Date')
            mve_df = shrout_df * close_df
            gbpvol_df = volume_df * close_df

            shrout_df = shrout_df.loc[:, shrout_df.columns[shrout_df.columns.isin(cols)]]
            close_df = close_df.loc[:, close_df.columns[close_df.columns.isin(cols)]]
            mve_df = mve_df.loc[:, mve_df.columns[mve_df.columns.isin(cols)]]
            gbpvol_df = gbpvol_df.loc[:, gbpvol_df.columns[gbpvol_df.columns.isin(cols)]]

            # error in TLW.L -> todo: edit those data and remerge
            close_df = close_df.replace(0, float('NaN')).fillna(method='ffill')

            close_df.to_csv(secd_ref_dir + 'close.csv')
            shrout_df.to_csv(secd_ref_dir + 'shrout.csv')
            mve_df.to_csv(secd_ref_dir + 'mve.csv')
            mve_df.to_csv(merge_dir + 'mve.csv')
            gbpvol_df.to_csv(secd_ref_dir + 'gbpvol.csv')
            gbpvol_df.to_csv(merge_dir + 'gbpvol.csv')

        print('Moved necessary data to input_secd!')
        useful_stuff.beep()

    # convert fixed date vs comp matrix into date vs ohlc matrix and save as {ric1(ticker)}.csv at /price_data_fixed/
    convertQ = convertQ
    if convertQ:
        df_c = pd.read_csv(merge_dir + adj_filename + 'close.csv')
        df_h = pd.read_csv(merge_dir + adj_filename + 'high.csv')
        df_l = pd.read_csv(merge_dir + adj_filename + 'low.csv')
        df_o = pd.read_csv(merge_dir + adj_filename + 'open.csv')
        files = useful_stuff.listdir(fixed_price_dir)

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

        useful_stuff.beep()
