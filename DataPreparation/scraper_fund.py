import os
import platform
import pandas as pd
import eikon as ek
import myEikon as mek
from prep_wrds import pivot
import Misc.useful_stuff as us
from time import sleep
from datetime import datetime

'''
1. Fetch fundamental data of periods FY/FS/FQ using eikon
2. Organise those data and move to ../data/processed/input_funda with each file containing date in rows,
                                                                                           companies in columns 
'''

metadata_dir = '../data/metadata/'
fund_data_dir = '../data/preprocessed/'
data_type = 'FQ'
if data_type == 'FY':
    suffix = ''
elif data_type == 'FS':
    suffix = 's'
else:  # FQ
    suffix = 'q'

fetchQ = True
appendQ = True
if platform.system() == 'Darwin':
    fetchQ = False
if appendQ:
    overwrite = False
    keep_both = False
    keep_orig = False
if fetchQ:
    data_type = 'FQ'
    # all_rics = pd.read_csv(metadata_dir + 'ref-comp.csv')['ric'].to_list()
    all_rics = pd.read_pickle(f'{metadata_dir}comp_list/available.pickle')['RIC'].to_list()
    # columns 'Green_name' and 'TR_name'
    green_tr_df = pd.read_csv(f'{metadata_dir}{"trs_additional" if appendQ else "trs_to_fetch"}.csv')
    fields = green_tr_df['TR_name']

    tl_dict = dict()
    for i in range(len(green_tr_df)):
        tl_dict[green_tr_df.loc[i, 'TR_name'].upper()] = green_tr_df.loc[i, 'Green_name'] + suffix

    # split into smaller list just in case it might give some blank rows
    slice_by = 500
    start_firm = 0
    for i in range(start_firm, len(all_rics), slice_by):
        rics = all_rics[i:i + slice_by]
        comps = mek.Companies(rics)

        # try until we can fetch some data
        goodenough = False
        while not goodenough:
            try:
                comps.fetch_data(fields, start='2000-01-01', period=data_type)
                if len(comps.get_history(-1)) > 0:
                    goodenough = True
                    df_21c = comps.get_history(-1)[0]
                    df_21c_raw = comps.get_history(-1, raw=True)[0]
                    comps.clear_history()
                sleep(1)
            except ek.EikonError as eke:
                goodenough = False
                print(eke.code, eke.message)
                print('try-except inside goodenough actually works!')
                sleep(1)
        goodenough = False
        while not goodenough:
            try:
                comps.fetch_data(fields, start='1980-01-01', end='1999-12-31', period=data_type)
                if len(comps.get_history(-1)) > 0:
                    goodenough = True
                    df_20c = comps.get_history(-1)[0]
                    df_20c_raw = comps.get_history(-1, raw=True)[0]
                sleep(1)
            except ek.EikonError as eke:
                goodenough = False
                print(eke.code, eke.message)
                print('try-except inside goodenough actually works!')
                sleep(1)

        # saving raw fetches just in case
        now = datetime.now().strftime("%y%m%d_%H%M%S")
        comps.set_history(pd.concat([df_21c, df_20c], axis=0))  # join vertically
        pd.concat([df_20c_raw, df_21c_raw], axis=0).to_csv(f'{fund_data_dir}_raw/{data_type}/raw_data_{i}_{now}.csv',
                                                           index=False)
        # divide, organize, and save
        for ric in rics:
            # columns: ['Instrument', 'TR.STH1', 'TR.STH1.DATE', 'TR.STH2', 'TR.STH2.DATE', ...]
            comp_df = comps.comp_specific_data(ric)
            comp_df = comp_df.drop('Instrument', axis=1)

            if not comp_df.isna().all().all():
                comp_df = comp_df  # debug point

            comp_df_new = comp_df.loc[:, comp_df.columns[0:2]]  # first data
            comp_df_new = comp_df_new.rename(columns={comp_df_new.columns[-1]: 'datadate'})

            dfs = []
            for acol in comp_df.columns[2:]:
                if acol.count('.') == 1:
                    to_concat = comp_df.loc[:, [acol + ".CALCDATE", acol]]
                    to_concat = to_concat.rename(columns={acol + ".CALCDATE": 'datadate'})
                    dfs.append(to_concat)
            comp_df_new = pd.concat(dfs)

            comp_df_new = comp_df_new.rename(columns=tl_dict)

            if appendQ:
                try:
                    original_df = pd.read_csv(f'{fund_data_dir}/{data_type}/{ric.replace(".", "-")}.csv')
                except FileNotFoundError:
                    original_df = pd.DataFrame(float('NaN'), columns=['datadate'], index=[0])

                # duplicate columns might exist, so resolve this issue
                common_col = comp_df_new.columns[comp_df_new.columns.isin(original_df.columns)]
                if 'datadate' in common_col:
                    common_col = common_col.drop('datadate')
                if (len(common_col) > 0) and (not overwrite) and (not keep_both) and (not keep_orig):
                    action = input(f'Following columns already exist:\n'
                                   f'{common_col.to_list()}\n'
                                   f'Which action would you like to perform? [overwrite/keep_both/keep_original] ')
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

                original_df = original_df.drop(common_col, axis=1) if overwrite else original_df
                comp_df_new = comp_df_new.drop(common_col, axis=1) if keep_orig else comp_df_new
                comp_df_new = original_df.merge(comp_df_new, on='datadate', how='outer')

            # manage duplicate 'datadate' (b/c more than one data released on one day)
            comp_df_new = comp_df_new.groupby('datadate').sum().reset_index()
            comp_df_new = comp_df_new.sort_values(by=['datadate'])

            # final cleanup
            comp_df_new = comp_df_new.replace(0, float('NaN'))  # fill empty columns
            comp_df_new = comp_df_new.replace('', float('NaN'))  # fill empty columns
            comp_df_new = comp_df_new.fillna(method='ffill')  # fill empty columns
            comp_df_new = comp_df_new.dropna(how='all')

            if len(comp_df_new) > 0:
                comp_df_new.to_csv(f'{fund_data_dir}{data_type}/{ric.replace(".", "-")}.csv', index=False)
                comp_df_new = comp_df_new.set_index('datadate')
                comp_df_new = comp_df_new.sort_index(axis=1)
                comp_df_new.to_csv(f'./files/fund_data/{data_type}/{ric.replace(".", "-")}.csv', index=True)

            print(ric, 'done!')
        print(f'{i} / {len(rics)} done!')

computeQ = True
if computeQ:
    fth_dir = f'../data/preprocessed/{data_type}/'
    files = us.listdir(fth_dir)

    print('Computing necessary columns...\n[', end='')
    for i, afile in enumerate(files):
        df = pd.read_csv(fth_dir + afile).fillna(0)

        # calculate some new columns
        df.loc[:, 'dcpstk' + suffix] = df.loc[:, 'pstk' + suffix] + df.loc[:, 'dcvt' + suffix]
        df.loc[:, 'txfo' + suffix] = df.loc[:, 'txt' + suffix] - df.loc[:, 'txfed' + suffix]
        if 'nopi1' + suffix in df.columns:
            df.loc[:, 'nopi' + suffix] = df.loc[:, 'nopi' + suffix] + df.loc[:, 'nopi1' + suffix]
            df = df.drop('nopi1' + suffix, axis=1)
        df.loc[:, 'xint' + suffix] = - df.loc[:, 'xint' + suffix]
        df.loc[:, 'capx' + suffix] = - df.loc[:, 'capx' + suffix]
        if 'rect1' + suffix in df.columns:
            df.loc[:, 'rect' + suffix] = df.loc[:, 'rect' + suffix] + df.loc[:, 'rect1' + suffix]
            df = df.drop('rect1' + suffix, axis=1)
        if 'invt2' + suffix in df.columns:
            df.loc[:, 'invt' + suffix] = df.loc[:, 'invt' + suffix].fillna(
                df.loc[:, 'invt2' + suffix])
            df = df.drop('invt2' + suffix, axis=1)
        df.loc[:, 'intan' + suffix] = df.loc[:, 'intan' + suffix] + df.loc[:, 'gdwlia' + suffix]
        df.loc[:, 'ao' + suffix] = df.loc[:, 'ao' + suffix] + df.loc[:, 'aco' + suffix]
        if 'fatl1' + suffix in df.columns:
            df.loc[:, 'fatl' + suffix] = df.loc[:, 'fatl' + suffix] + df.loc[:, 'fatl1' + suffix]
            df = df.drop('fatl1' + suffix, axis=1)
        if ('dlc1' + suffix in df.columns) and ('dlc11' + suffix in df.columns):
            df.loc[:, 'dltt' + suffix] = df.loc[:, 'dlc1' + suffix] + df.loc[:, 'dlc11' + suffix]
            df.loc[:, 'dlc' + suffix] = df.loc[:, 'dlc' + suffix] + df.loc[:, 'dltt' + suffix]
            df = df.drop(['dlc1' + suffix, 'dlc11' + suffix], axis=1)
        if 'lt1' + suffix in df.columns:
            df.loc[:, 'lt' + suffix] = df.loc[:, 'lt' + suffix] - df.loc[:, 'lt1' + suffix]
            df = df.drop('lt1' + suffix, axis=1)

        # lines 86-92
        df.loc[:, 'dr' + suffix] = df.loc[:, 'drlt' + suffix] + df.loc[:, 'drc' + suffix]
        df.loc[:, 'dc' + suffix] = df.loc[:, 'dcvt' + suffix]

        # multiplier is for calculating the 'count' column
        if data_type == 'FY':
            multiplier = 1
        elif data_type == 'FS':
            multiplier = 2
        else:  # FQ
            multiplier = 4
        df['count' + suffix] = (multiplier * (pd.to_datetime(df['datadate']) - pd.to_datetime(df['datadate']).min()
                                              ).dt.days) // 365 + 1

        df = df.replace(0, float('NaN'))
        df = df.set_index('datadate')
        df = df.sort_index(axis=1)
        df.to_csv(fth_dir + afile, index=True)

        if i % 20 == 19:
            print('-', end='')
        if i % 500 == 499:
            print(f'] {i + 1} / {len(files)}\n[', end='')
    print(f'] {len(files)} / {len(files)}\n')

# organise & move to /by_data/from_ref
to_organize = ['FQ']
for Fsth in to_organize:
    if Fsth == 'FY':
        suffix = 'a'
    elif Fsth == 'FS':
        suffix = 's'
    else:  # FQ
        suffix = 'q'
    date_col = pd.read_csv(metadata_dir + 'business_days.csv').rename(columns={'YYYY-MM-DD': 'datadate'}
                                                                      ).loc[:, 'datadate']
    fsth_dir = fund_data_dir + Fsth + '/'
    files = us.listdir(fsth_dir)
    firms = [file_name[:-4].replace('-', '.') for file_name in files]

    dfs = []
    for i, file_name in enumerate(files):
        to_concat = pd.read_csv(fsth_dir + file_name)
        to_concat['ric'] = firms[i]
        dfs.append(to_concat)
    total_df = pd.concat(dfs, axis=0)
    print('Finished loading data!')

    final_output_dir = f'../data/processed/input_fund{suffix}/'

    date_col_name = us.date_col_finder(total_df, 'total_df')

    fields = total_df.columns.drop([date_col_name, 'ric'])
    for field_name in fields:
        field_df = total_df.loc[:, [date_col_name, 'ric', field_name]]
        field_df = pivot(field_df)

        field_df = field_df.merge(date_col, on=date_col_name, how='outer').sort_values(date_col_name)
        last_nans = ~field_df.fillna(method='bfill').notna()  # True if last NaNs
        is_del_series = field_df.columns.str.contains('^')  # True if delisted
        last_nans = last_nans.apply(lambda x: x * is_del_series, axis=1)  # True if delisted last NaNs
        field_df = field_df.fillna(method='ffill') * (~last_nans)

        field_df.to_csv(final_output_dir + f'{field_name}.csv', index=False)
        print(f'{field_name} done!')
