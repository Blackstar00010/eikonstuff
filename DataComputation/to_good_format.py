import shutil
import pandas as pd
from Misc import useful_stuff as us
from os.path import join as pathjoin

wrds = True
secd_dir = '../data/processed_wrds/input_secd/' if wrds else '../data/processed/input_secd/'
by_var_dd_dir = '../data/processed_wrds/output_by_var_dd/' if wrds else '../data/processed/output_by_var_dd/'
by_var_mm_dir = '../data/processed_wrds/output_by_var_mm/' if wrds else '../data/processed/output_by_var_mm/'
by_month_dir = '../data/processed_wrds/output_by_month/' if wrds else '../data/processed/output_by_month/'
final_dir = '../data/processed_wrds/output/' if wrds else '../data/processed/output/'

close_to_momentum, dd_to_mm, mm_to_month = True, False, True

if close_to_momentum:
    try:
        close_df = pd.read_csv(pathjoin(secd_dir, 'close_adj.csv'))
    except FileNotFoundError:
        close_df = pd.read_csv(pathjoin(secd_dir, 'close.csv'))
    date_col_name = us.date_col_finder(close_df, 'close')
    close_df.loc[:, date_col_name] = us.datetime_to_str(close_df.loc[:, date_col_name])
    close_df = close_df.sort_values(by=date_col_name)

    months = close_df[date_col_name].apply(lambda x: x[5:7])
    close_df = close_df.loc[close_df.index[months != months.shift(1)], :]

    close_df = close_df.set_index(date_col_name)
    close_df = close_df.replace(0, float('NaN'))
    close_df = us.fillna(close_df, hat_in_cols=True).replace(float('NaN'), 0)

    print('[', end='')
    for current_date in close_df.index[50:]:
        if current_date == '1991-06-01':
            current_date = current_date  # debug point
        window = close_df.loc[:current_date].tail(50)

        mom = pd.DataFrame(float('NaN'), index=range(1, 50), columns=window.columns)

        mom.loc[1] = window.iloc[-1] / window.iloc[-2] - 1
        for i in range(2, 50):
            mom.loc[i] = window.iloc[-2] / window.shift(i - 1).iloc[-2] - 1
        mom = mom.T
        mom = mom.rename(columns={i: f'Mom_{i}' for i in mom.columns})

        mom = mom.replace([float('inf'), -float('inf'), -1], float('NaN'))
        mom = mom.dropna(how='all', axis=0)
        mom.to_csv(by_month_dir + current_date[:-3] + '.csv', index_label='firms')

        print('-', end='')
        if int(current_date[5:7]) == 12:
            print(f'] {current_date[:7]} done!\n[', end='')
    if int(current_date[5:7]) != 12:
        print(f'] {current_date[:7]} done!\n')

if dd_to_mm:
    for afile in us.listdir(by_var_dd_dir):
        shutil.copyfile(by_var_dd_dir + afile, by_var_mm_dir + afile)

    print('Finding outer-joined date indexes...')
    # da_row = us.outer_joined_axis(by_var_mm_dir, file_type='csv', axis='daterow')
    # updating business_days.csv
    # bday_df = pd.read_csv('../data/metadata/business_days.csv').set_index('YYYY-MM-DD')
    # to_concat = pd.DataFrame(float('NaN'), columns=bday_df.columns, index=da_row[~da_row.isin(bday_df.index)])
    # bday_df = pd.concat([bday_df, to_concat]).sort_index().reset_index().rename(columns={'index': 'YYYY-MM-DD'})
    # bday_df.loc[:, 'YYYY'] = bday_df.loc[:, 'YYYY-MM-DD'].str[:4]
    # bday_df.loc[:, 'MM'] = bday_df.loc[:, 'YYYY-MM-DD'].str[5:7]
    # bday_df.loc[:, 'DD'] = bday_df.loc[:, 'YYYY-MM-DD'].str[8:10]
    # bday_df['YYYYMMDD'] = bday_df['YYYY'] + bday_df['MM'] + bday_df['DD']
    # bday_df['YYYY/MM/DD'] = bday_df['YYYY'] + '/' + bday_df['MM'] + '/' + bday_df['DD']
    # bday_df.to_csv('../data/metadata/business_days.csv', index=False)
    # print('Finding outer-joined columns...')
    # da_col = us.outer_joined_axis(by_var_mm_dir, file_type='csv', axis='column')
    #
    # print('Reformatting files...')
    # for afile in us.listdir(by_var_mm_dir):
    #     df = pd.read_csv(pathjoin(by_var_mm_dir, afile))
    #     df = df.set_index(us.date_col_finder(df, afile))
    #     if len(df) == len(da_row):
    #         continue
    #     to_concat = pd.DataFrame(float('NaN'),
    #                              columns=da_col[~da_col.isin(df.columns)],
    #                              index=da_row[~da_row.isin(df.index)])
    #     df = pd.concat([df, to_concat])
    #     df = df.sort_index()
    #     last_nan_mask = df.fillna(method='bfill').notna()
    #     df = df.fillna(method='ffill') * last_nan_mask
    #     df.to_csv(pathjoin(by_var_mm_dir, afile), index=True, index_label='datadate')
    #     print(f'Finished reformatting {afile}!')

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
    df = []
    for avar in var_list:
        to_concat = pd.read_csv(pathjoin(by_var_mm_dir, avar))
        valid_count = (to_concat.notna() * to_concat!=0).sum().sum()
        if (valid_count / (to_concat.shape[0] * to_concat.shape[1])) < 0.8:
            continue
        to_concat['var'] = avar[:-4]
        df.append(to_concat)
        print(f'{avar} imported!')
    df = pd.concat(df, axis=0)
    # df's columns: [datadate, *.L, *.L^***, var]

    date_col_name = us.date_col_finder(df, 'df')
    first_days = df[date_col_name].unique()
    first_days = sorted(first_days)
    for afirst_day in first_days:
        yyyy_mm = afirst_day[:-3]
        if (yyyy_mm + '.csv') not in us.listdir(by_month_dir):
            print(f'{yyyy_mm} skipped!')
            continue
        moms_df = pd.read_csv(pathjoin(by_month_dir, yyyy_mm + '.csv'))
        moms_df = moms_df.set_index('firms')
        moms_df = moms_df.dropna(how='any', axis=0)

        chars_df = df.loc[df[date_col_name] == afirst_day, :]
        chars_df = chars_df.drop(date_col_name, axis=1).rename(columns={'var': 'firms'}).set_index('firms')
        chars_df = chars_df.transpose()
        for acol in chars_df:
            if acol in moms_df:
                chars_df = chars_df.drop(acol, axis=1)

        best_case_finder = []
        chars_df_og = chars_df
        for thresh_col in range(10, 1, -1):
            chars_df = chars_df_og
            # drop columns with too many NaNs
            for acol in chars_df.columns:
                # shitty column
                if chars_df[acol].notna().sum() < (len(chars_df) * thresh_col/10):
                    chars_df = chars_df.drop(acol, axis=1)

            # drop rows with too many NaNs
            thresh_row = 1.0
            temp_df = chars_df.dropna(thresh=int(chars_df.shape[1] * thresh_row), axis=0)
            while len(temp_df) < (len(chars_df)/2):
                thresh_row -= 0.1
                temp_df = chars_df.dropna(thresh=int(chars_df.shape[1] * thresh_row), axis=0)
            temp_df = temp_df.dropna(how='any', axis=1)

            best_case_finder.append([thresh_col, thresh_row, temp_df.shape[0] * temp_df.shape[1]])
        best_case_finder = pd.DataFrame(best_case_finder, columns=['thresh_col', 'thresh_row', 'valid_count'])
        best_case_finder = best_case_finder.sort_values(by=['valid_count', 'thresh_col'], ascending=False)

        chars_df = chars_df_og
        for acol in chars_df.columns:
            if chars_df[acol].notna().sum() < (len(chars_df) * best_case_finder.iloc[0, 0] / 10):
                chars_df = chars_df.drop(acol, axis=1)
        chars_df = chars_df.dropna(thresh=int(chars_df.shape[1] * best_case_finder.iloc[0, 1]), axis=0)
        chars_df = chars_df.dropna(how='any', axis=1)

        joined_df = pd.concat([chars_df, moms_df], axis=1)
        joined_df = joined_df.dropna(how='all', axis=1).dropna(how='any', axis=0)
        joined_df.to_csv(pathjoin(by_month_dir, yyyy_mm + '.csv'), index=True, index_label='firms')
        print(f'{yyyy_mm} exported!')
