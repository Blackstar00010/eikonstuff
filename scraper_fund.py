import os
import pandas as pd
import eikon as ek
import myEikon as mek
from wrds_prep import pivot
from time import sleep

ref_comp_df = pd.read_csv('files/metadata/ref-comp.csv')
all_rics = ref_comp_df['ric'].to_list()
shits = pd.read_csv('files/metadata/supp_data.csv')
tl_dict = {}
fields = shits['TR_name'].to_list()
for i in range(len(shits)):
    tl_dict[shits['TR_name'][i].upper()] = shits['shitty_name'][i]
data_type = 'FS'
date_col = pd.read_csv('files/metadata/business_days.csv').rename(columns={'YYYY-MM-DD': 'datadate'}).loc[:, 'datadate']

slice_by = 50
start_firm = 1350
# split into smaller list just in case it might give some blank rows
fetchQ = True
if fetchQ:
    for i in range(start_firm, len(all_rics), slice_by):
        rics = all_rics[i:i + slice_by]
        comps = mek.Companies(rics)

        # try until we can fetch some data
        goodenough = False
        while not goodenough:
            try:
                sleep(1)
                comps.fetch_data(fields, start='2000-01-01', period=data_type)
                if len(comps.get_history(-1)) > 0:
                    goodenough = True
                    df_21c = comps.get_history(-1)[0]
                    df_21c_raw = comps.get_history(-1, raw=True)[0]
                    comps.clear_history()
            except ek.EikonError:
                goodenough = False
        goodenough = False
        while not goodenough:
            try:
                sleep(1)
                comps.fetch_data(fields, start='1980-01-01', end='1999-12-31', period=data_type)
                if len(comps.get_history(-1)) > 0:
                    goodenough = True
                    df_20c = comps.get_history(-1)[0]
                    df_20c_raw = comps.get_history(-1, raw=True)[0]
                    comps.set_history(pd.concat([df_21c, df_20c], axis=0))
                    pd.concat([df_20c_raw, df_21c_raw], axis=0).to_csv(f'files/fund_data/_raw/{data_type}/raw_data_{i}.csv',
                                                                       index=False)
            except ek.EikonError:
                goodenough = False

        # separate, arrange, and save
        for ric in rics:
            # N=len(tr_dict) number of value columns, N number of date columns
            comp_df = comps.comp_specific_data(ric)
            # columns: ['Instrument', 'TR.STH1', 'TR.STH1.DATE', 'TR.STH2', 'TR.STH2.DATE', ...]

            comp_df_new = comp_df.loc[:, comp_df.columns[0:3]]  # first data
            comp_df_new = comp_df_new.rename(columns={comp_df_new.columns[-1]: 'datadate'})
            for acol in comp_df.columns[3:]:
                if acol.count('.') == 1:
                    to_concat = comp_df.loc[:, [acol + ".CALCDATE", acol]]
                    to_concat = to_concat.rename(columns={acol + ".CALCDATE": 'datadate'})
                    comp_df_new = pd.concat([comp_df_new, to_concat])

            # manage duplicate 'datadate'
            comp_df_new = comp_df_new.dropna(how='all')
            comp_df_new = comp_df_new.groupby('datadate').sum().reset_index()
            comp_df_new = comp_df_new.sort_values(by=['datadate'])

            # rename columns & adding columns
            comp_df_new = comp_df_new.rename(columns=tl_dict)
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

            if data_type == 'FQ':
                comp_df_new = comp_df_new.rename(columns={col: col + 'q' for col in comp_df_new.columns
                                                          if col not in ['datadate', 'Instrument']})
            if data_type == 'FS':
                comp_df_new = comp_df_new.rename(columns={col: col + 's' for col in comp_df_new.columns
                                                          if col not in ['datadate', 'Instrument']})

            # final cleanup
            comp_df_new = comp_df_new.drop(['Instrument'], axis=1)
            comp_df_new = comp_df_new.replace(0, float('NaN'))  # fill empty columns
            comp_df_new = comp_df_new.replace('', float('NaN'))  # fill empty columns
            comp_df_new = comp_df_new.fillna(method='ffill')  # fill empty columns
            comp_df_new = comp_df_new.dropna(how='all')
            if len(comp_df_new) > 0:
                comp_df_new.to_csv(f'./files/fund_data/{data_type}/{ric.replace(".", "-")}.csv', index=False)

            print(ric, 'done!')

# organise & move to /by_data/from_ref
organize_FY = False
if organize_FY:
    fy_dir = 'files/fund_data/FY/'
    files = os.listdir(fy_dir)
    firms = [file_name[:-4].replace('-', '.') for file_name in files]

    total_df = pd.DataFrame()
    for i, file_name in enumerate(files):
        to_concat = pd.read_csv(fy_dir + file_name)
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
