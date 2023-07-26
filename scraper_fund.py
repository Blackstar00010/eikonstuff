import os
import pandas as pd
import eikon as ek
import myEikon as mek
from wrds_prep import pivot
from time import sleep
from datetime import datetime

'''
1. Fetch FY/FS/FQ data using eikon
2. Organise those data and move to /by_data/from_ref with each file of certain data containing date in rows companies in columns 
'''

# split into smaller list just in case it might give some blank rows
fetchQ = True
appendQ = True
if appendQ:
    overwrite = False
    keep_both = False
    keep_orig = False
if fetchQ:
    data_type = 'FQ'
    all_rics = pd.read_csv('files/metadata/ref-comp.csv')['ric'].to_list()
    # columns 'Green_name' and 'TR_name'
    green_tr_df = pd.read_csv('files/metadata/trs_additional.csv') if appendQ else pd.read_csv('files/metadata/'
                                                                                               'trs_to_fetch.csv')
    tl_dict = dict()
    fields = green_tr_df['TR_name']
    for i in range(len(green_tr_df)):
        tl_dict[green_tr_df.loc[i, 'TR_name'].upper()] = green_tr_df.loc[i, 'Green_name']

    slice_by = 25
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
        pd.concat([df_20c_raw, df_21c_raw], axis=0).to_csv(f'files/fund_data/_raw/{data_type}/raw_data_{i}_{now}.csv',
                                                           index=False)
        # divide, organize, and save
        for ric in rics:
            # columns: ['Instrument', 'TR.STH1', 'TR.STH1.DATE', 'TR.STH2', 'TR.STH2.DATE', ...]
            comp_df = comps.comp_specific_data(ric)

            comp_df_new = comp_df.loc[:, comp_df.columns[0:3]]  # first data
            comp_df_new = comp_df_new.rename(columns={comp_df_new.columns[-1]: 'datadate'})
            for acol in comp_df.columns[3:]:
                if acol.count('.') == 1:
                    to_concat = comp_df.loc[:, [acol + ".CALCDATE", acol]]
                    to_concat = to_concat.rename(columns={acol + ".CALCDATE": 'datadate'})
                    comp_df_new = pd.concat([comp_df_new, to_concat])

            comp_df_new = comp_df_new.rename(columns=tl_dict)

            if appendQ:
                original_df = pd.read_csv(f'files/fund_data/{data_type}/{ric.replace(".", "-")}.csv')

                # duplicate columns might exist so resolve this
                common_col = comp_df_new.columns[comp_df_new.columns.isin(original_df.columns)][1:]  # except datadate
                if (len(common_col) > 1) and (not overwrite) and (not keep_both) and (not keep_orig):
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
            comp_df_new = comp_df_new.dropna(how='all')
            comp_df_new = comp_df_new.groupby('datadate').sum().reset_index()
            comp_df_new = comp_df_new.sort_values(by=['datadate'])

            # calculate new columns
            comp_df_new.loc[:, 'dcpstk'] = comp_df_new.loc[:, 'pstk'] + comp_df_new.loc[:, 'dcvt']
            comp_df_new.loc[:, 'txfo'] = comp_df_new.loc[:, 'txt'] - comp_df_new.loc[:, 'txfed']
            comp_df_new.loc[:, 'nopi'] = comp_df_new.loc[:, 'nopi'] + comp_df_new.loc[:, 'nopi1']
            comp_df_new = comp_df_new.drop('nopi1', axis=1)
            comp_df_new.loc[:, 'xint'] = - comp_df_new.loc[:, 'xint']
            comp_df_new.loc[:, 'capx'] = - comp_df_new.loc[:, 'capx']
            comp_df_new.loc[:, 'rect'] = comp_df_new.loc[:, 'rect'] + comp_df_new.loc[:, 'rect1']
            comp_df_new = comp_df_new.drop('rect1', axis=1)
            comp_df_new.loc[:, 'invt'] = comp_df_new.loc[:, 'invt'].fillna(comp_df_new.loc[:, 'invt2'])
            comp_df_new = comp_df_new.drop('invt2', axis=1)
            comp_df_new.loc[:, 'intan'] = comp_df_new.loc[:, 'intan'] + comp_df_new.loc[:, 'gdwlia']
            comp_df_new.loc[:, 'ao'] = comp_df_new.loc[:, 'ao'] + comp_df_new.loc[:, 'aco']
            comp_df_new.loc[:, 'fatl'] = comp_df_new.loc[:, 'fatl'] + comp_df_new.loc[:, 'fatl1']
            comp_df_new = comp_df_new.drop('fatl1', axis=1)
            comp_df_new.loc[:, 'dltt'] = comp_df_new.loc[:, 'dlc1'] + comp_df_new.loc[:, 'dlc11']
            comp_df_new.loc[:, 'dlc'] = comp_df_new.loc[:, 'dlc'] + comp_df_new.loc[:, 'dltt']
            comp_df_new = comp_df_new.drop(['dlc1', 'dlc11'], axis=1)
            comp_df_new.loc[:, 'lt'] = comp_df_new.loc[:, 'lt'] - comp_df_new.loc[:, 'lt1']
            comp_df_new = comp_df_new.drop('lt1', axis=1)

            # lines 86-92
            comp_df_new.loc[:, 'dr'] = comp_df_new.loc[:, 'drlt'] + comp_df_new.loc[:, 'drc']
            comp_df_new.loc[:, 'dc'] = comp_df_new.loc[:, 'dcvt']

            # final cleanup
            comp_df_new = comp_df_new.drop(['Instrument'], axis=1)
            comp_df_new = comp_df_new.replace(0, float('NaN'))  # fill empty columns
            comp_df_new = comp_df_new.replace('', float('NaN'))  # fill empty columns
            comp_df_new = comp_df_new.fillna(method='ffill')  # fill empty columns
            comp_df_new = comp_df_new.dropna(how='all')

            # adding a column that counts the firm's age
            multiplier = 1
            if data_type == 'FQ':
                multiplier = 4
            elif data_type == 'FS':
                multiplier = 2
            comp_df_new['count'] = (multiplier * (pd.to_datetime(comp_df_new['datadate']
                                                                 ) - pd.to_datetime(comp_df_new['datadate']).min()
                                                  ).dt.days) // 365 + 1

            # rename columns based on accounting period
            if data_type == 'FQ':
                comp_df_new = comp_df_new.rename(columns={col: col + 'q' for col in comp_df_new.columns
                                                          if col != 'datadate'})
            if data_type == 'FS':
                comp_df_new = comp_df_new.rename(columns={col: col + 's' for col in comp_df_new.columns
                                                          if col != 'datadate'})

            if len(comp_df_new) > 0:
                comp_df_new.to_csv(f'./files/fund_data/{data_type}/{ric.replace(".", "-")}.csv', index=False)

            print(ric, 'done!')
        print(f'{i} / {len(rics)} done!')

# organise & move to /by_data/from_ref
to_organize = []
# to_organize = ['FY']
for Fsth in to_organize:
    date_col = pd.read_csv('files/metadata/business_days.csv').rename(columns={'YYYY-MM-DD': 'datadate'}
                                                                      ).loc[:, 'datadate']
    fsth_dir = f'files/fund_data/{Fsth}/'
    files = os.listdir(fsth_dir)
    firms = [file_name[:-4].replace('-', '.') for file_name in files]

    total_df = pd.DataFrame()
    for i, file_name in enumerate(files):
        to_concat = pd.read_csv(fsth_dir + file_name)
        to_concat['ric'] = firms[i]
        total_df = pd.concat([total_df, to_concat], axis=0)
    print('Finished loading data!')

    fields = total_df.columns.drop(['datadate', 'ric'])
    for field_name in fields:
        field_df = total_df.loc[:, ['datadate', 'ric', field_name]]
        field_df = pivot(field_df)

        field_df = field_df.merge(date_col, on='datadate', how='outer').sort_values('datadate')
        field_df = field_df.fillna(method='ffill')
        field_df.to_csv(f'files/by_data/from_ref/{field_name}.csv', index=False)
        print(f'{field_name} done!')
