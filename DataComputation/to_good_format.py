import shutil
import pandas as pd
from Misc import useful_stuff as us
from os.path import join as pathjoin

by_var_dd_dir = '../data/processed/output_by_var_dd/'
by_var_mm_dir = '../data/processed/output_by_var_mm/'
by_month_dir = '../data/processed/output_by_month/'
final_dir = '../data/processed/output/'

dd_to_mm, mm_to_month, join_price = True, True, False

if dd_to_mm:
    for afile in us.listdir(by_var_dd_dir):
        shutil.copyfile(by_var_dd_dir + afile, by_var_mm_dir + afile)

    print('Finding outer-joined date indexes...')
    da_row = us.outer_joined_axis(by_var_mm_dir, file_type='csv', axis='daterow')
    # updating business_days.csv
    bday_df = pd.read_csv('../data/metadata/business_days.csv').set_index('YYYY-MM-DD')
    to_concat = pd.DataFrame(float('NaN'), columns=bday_df.columns, index=da_row[~da_row.isin(bday_df.index)])
    bday_df = pd.concat([bday_df, to_concat]).sort_index().reset_index().rename(columns={'index': 'YYYY-MM-DD'})
    bday_df.loc[:, 'YYYY'] = bday_df.loc[:, 'YYYY-MM-DD'].str[:4]
    bday_df.loc[:, 'MM'] = bday_df.loc[:, 'YYYY-MM-DD'].str[5:7]
    bday_df.loc[:, 'DD'] = bday_df.loc[:, 'YYYY-MM-DD'].str[8:10]
    bday_df['YYYYMMDD'] = bday_df['YYYY'] + bday_df['MM'] + bday_df['DD']
    bday_df['YYYY/MM/DD'] = bday_df['YYYY'] + '/' + bday_df['MM'] + '/' + bday_df['DD']
    bday_df.to_csv('../data/metadata/business_days.csv', index=False)
    print('Finding outer-joined columns...')
    da_col = us.outer_joined_axis(by_var_mm_dir, file_type='csv', axis='column')

    print('Reformatting files...')
    for afile in us.listdir(by_var_mm_dir):
        df = pd.read_csv(pathjoin(by_var_mm_dir, afile))
        df = df.set_index(us.date_col_finder(df, afile))
        if len(df) == len(da_row):
            continue
        to_concat = pd.DataFrame(float('NaN'),
                                 columns=da_col[~da_col.isin(df.columns)],
                                 index=da_row[~da_row.isin(df.index)])
        df = pd.concat([df, to_concat])
        df = df.sort_index()
        last_nan_mask = df.fillna(method='bfill').notna()
        df = df.fillna(method='ffill') * last_nan_mask
        df.to_csv(pathjoin(by_var_mm_dir, afile), index=True, index_label='datadate')
        print(f'Finished reformatting {afile}!')

    print('Calculating first days of months...')
    for afile in us.listdir(by_var_mm_dir):
        df = pd.read_csv(by_var_mm_dir + afile)
        date_vec = df[us.date_col_finder(df, df_name=afile)]
        date_vec = us.datetime_to_str(date_vec)
        date_vec = date_vec.apply(lambda x: x[5:7])  # YYYY-MM-DD
        mask = date_vec == date_vec.shift(1)
        df = df.loc[df.index[~mask], :]
        if 'Unnamed: 0' in df.columns:
            df = df.drop('Unnamed: 0', axis=1)
        df.to_csv(pathjoin(by_var_mm_dir, afile), index=False)
        print(f'Finished calculating first days of {afile}!')

if mm_to_month:
    var_list = us.listdir(by_var_mm_dir)
    df = pd.DataFrame()
    for avar in var_list:
        to_concat = pd.read_csv(pathjoin(by_var_mm_dir, avar))
        to_concat['var'] = avar[:-4]
        df = pd.concat([df, to_concat], axis=0)
        print(f'{avar} imported!')
    # df's columns: [datadate, *.L, *.L^***, var]

    date_col = us.date_col_finder(df, 'df')
    first_days = df[date_col].unique()
    for afirst_day in first_days:
        yyyy_mm = afirst_day[:-3]
        this_month_df: pd.DataFrame = df.loc[df[date_col] == afirst_day, :]
        this_month_df = this_month_df.drop(date_col, axis=1).set_index('var')
        this_month_df = this_month_df.transpose()
        this_month_df.to_csv(pathjoin(by_month_dir, yyyy_mm+'.csv'), index=True)
        print(f'{yyyy_mm} exported!')
