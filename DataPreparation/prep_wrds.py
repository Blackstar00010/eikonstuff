import os
import Misc.useful_stuff as us
import pandas as pd
import multiprocessing as mp
# from joblib import Parallel, delayed

all_dir = '/Users/jamesd/Desktop/wrds_data/all/'
useful_dir = '/Users/jamesd/Desktop/wrds_data/useful/'
raw_output_dir = '../data/preprocessed_wrds/_raw/'
preprocessed_dir = '../data/preprocessed_wrds/'
processed_dir = '../data/processed_wrds/'


def shitty_pivot(some_df: pd.DataFrame) -> pd.DataFrame:
    """
    Deprecated. Use pivot() method.
    :param some_df: dataframe to pivot
    :return: pivoted dataframe
    """
    gvkeys = some_df['gvkey'].drop_duplicates(keep='first')
    dataname = some_df.columns[-1]
    ret = some_df[some_df['gvkey'] == gvkeys[0]].drop_duplicates(subset='datadate', keep='last')
    ret = ret.rename(columns={dataname: gvkeys[0]}).drop('gvkey', axis=1)
    for gvkey in gvkeys[1:]:
        new_df = some_df[some_df['gvkey'] == gvkey].drop_duplicates(subset='datadate', keep='last')
        new_df = new_df.rename(columns={dataname: gvkey}).drop('gvkey', axis=1)
        ret = pd.merge(ret, new_df, on='datadate', how='outer')
    return ret


def pivot(some_df: pd.DataFrame) -> pd.DataFrame:
    """
    Only for pivoting df with columns ['ric', `date`, `data`] into df of columns of rics and rows of datadates
    :param some_df: dataframe to pivot
    :return: pivoted dataframe
    """
    date_col_name = us.date_col_finder(some_df, 'df_to_pivot')
    value = some_df.columns.drop([date_col_name, 'ric'])
    ret = some_df.pivot_table(index=date_col_name, columns='ric', values=value)
    ret.columns = ret.columns.droplevel(level=0)  # b/c of two-level column names
    ret = ret.sort_index(ascending=True)
    return ret


def gvkey2ric(some_df: pd.DataFrame, gvkey_ric_dict: dict = None, add_hat: bool = False) -> pd.DataFrame:
    """
    :param some_df: dataframe with gvkey column
    :param gvkey_ric_dict: dictionary of {gvkey1: ric1, ...}
    :param add_hat: whether to add hat and last valid data's date to ric
    :return: dataframe with gvkey column replaced by ric column
    """
    if gvkey_ric_dict is not None:
        some_df = some_df[some_df['gvkey'].isin(list(gvkey_ric_dict.keys()))]
        some_df.loc[:, 'ric'] = some_df.loc[:, 'gvkey'].map(gvkey_ric_dict)
        some_df = some_df.drop('gvkey', axis=1)
    else:
        some_df.loc[:, 'ric'] = some_df.loc[:, 'gvkey'].apply(us.num2ric)
        some_df = some_df.drop('gvkey', axis=1)
        if add_hat:
            date_col_name = us.date_col_finder(some_df, 'df_in_gvkey2ric()')
            del_code = some_df[['ric', date_col_name]].drop_duplicates(subset='ric', keep='last')
            del_code['suffix_YYYY'] = del_code[date_col_name].str[:4].astype(int)
            del_code['suffix_MM'] = del_code[date_col_name].str[5:7].astype(int)
            del_code['suffix'] = ('^' + del_code['suffix_MM'].apply(lambda x: str(chr(x + 64))) +
                                  (del_code['suffix_YYYY'] % 100).astype(str).apply(lambda x: x.rjust(2, '0'))) * \
                                 ((del_code['suffix_YYYY'] < 2023) + (
                                         (del_code['suffix_YYYY'] == 2023) * (del_code['suffix_MM'] < 7)))
            del_code['ric_new'] = del_code['ric'] + del_code['suffix']
            del_code = del_code.drop([date_col_name, 'suffix_YYYY', 'suffix_MM', 'suffix'], axis=1)
            some_df = some_df.merge(del_code, on='ric', how='left')
            some_df = some_df.drop('ric', axis=1).rename(columns={'ric_new': 'ric'})
    return some_df


def collapse_dup(some_df: pd.DataFrame, subset: list = None, df_name: str = None) -> pd.DataFrame:
    """
    Use parallel computing to groupby based on subset and collapse duplicates
    :param some_df: dataframe with duplicated rows
    :param subset: subset of columns to consider duplicates
    :param df_name: name of dataframe. If not None, prints progress
    :return: dataframe with duplicates collapsed
    """
    if subset is None:
        subset = some_df.columns
    ret = some_df.sort_values(by=subset).reset_index(drop=True)
    dirty_index = ret.index[ret.duplicated(subset, keep=False)]
    dirty_part = ret.loc[dirty_index, :]
    ret = ret.drop(dirty_index, axis=0)
    dirty_part = dirty_part.groupby(subset).apply(
        lambda group: group.fillna(method='ffill')).reset_index(drop=True)
    dirty_part = dirty_part.drop_duplicates(subset=subset, keep='last')
    ret = pd.concat([ret, dirty_part], axis=0).sort_values(by=subset).reset_index(drop=True)
    if df_name is not None:
        print(f'Finished managing duplicates of {df_name} data!')
    return ret


if __name__ == "__main__":
    # metadata
    mix_ref = False
    problematic_before = '1985-01-01'  # only data from this date

    # gvkeys -> RIC dictionary
    ref_comp_table = pd.read_csv('../data/metadata/ref-comp.csv')[['ric', 'gvkey']]
    gvkey_ric_dict = {}
    for i in range(len(ref_comp_table)):
        gvkey_ric_dict[ref_comp_table.loc[i, 'gvkey']] = ref_comp_table.loc[i, 'ric']

    # GBPXXX
    exch_rates = pd.read_csv(all_dir + 'comp_exrt_all.csv', low_memory=False)
    date_col_name = us.date_col_finder(exch_rates, 'exch_rates')

    exch_rates = exch_rates.loc[:, ['tocurd', 'exratd', date_col_name]]
    exch_rates = exch_rates.rename(columns={'tocurd': 'curr', 'exratd': 'GBPXXX'})
    exch_rates[date_col_name] = us.dt_to_str(exch_rates[date_col_name].astype(str))
    exch_rates = exch_rates.loc[exch_rates[date_col_name] >= problematic_before, :].reset_index(drop=True)

    exch_rates['GBPXXX'] = exch_rates['GBPXXX'].replace(0, float('NaN'))
    exch_rates[['curr', 'GBPXXX']] = exch_rates[['curr', 'GBPXXX']].groupby('curr').apply(
        lambda group: group['GBPXXX'].fillna(method='ffill')
    ).reset_index(level=0)
    exch_rates = exch_rates.dropna(subset=['curr', 'GBPXXX'], how='any')

    organize_secd = False
    if organize_secd:
        # gvkey,datadate,conm,isin,exchg,monthend,curcdd,ajexdi,cshoc,cshtrd,
        # prccd,prchd,prcld,prcod,curcddv,divd,paydate,split
        comp_secd = pd.read_csv(useful_dir + 'comp_secd.csv', low_memory=False)
        comp_secd = comp_secd.drop(['conm', 'isin', 'exchg'], axis=1)
        comp_secd[['prccd', 'prchd', 'prcld', 'prcod'
                   ]] = comp_secd[['prccd', 'prchd', 'prcld', 'prcod']].replace(0, float('NaN'))

        # changing date format
        date_col_name = us.date_col_finder(comp_secd, 'comp_secd')
        comp_secd.loc[:, date_col_name] = us.dt_to_str(comp_secd.loc[:, date_col_name])
        comp_secd = comp_secd.loc[comp_secd[date_col_name] >= problematic_before, :].reset_index(drop=True)

        # remove duplicates
        comp_secd = collapse_dup(comp_secd, subset=['gvkey', date_col_name], df_name='comp_secd')
        # group ffill
        comp_secd[['gvkey', 'curcdd', 'prccd']] = comp_secd[['gvkey', 'curcdd', 'prccd']].groupby('gvkey').apply(
            lambda group: group[['curcdd', 'prccd']].fillna(method='ffill')
        ).reset_index(level=0)
        comp_secd = comp_secd.dropna(subset=['curcdd', 'prccd'], how='any')

        # gvkey -> ric, reformatting
        comp_secd = gvkey2ric(comp_secd, gvkey_ric_dict) if mix_ref else gvkey2ric(comp_secd, add_hat=True)
        comp_secd = comp_secd.rename(columns={'curcdd': 'curr', 'curcddv': 'curr_div',
                                              'ajexdi': 'adj', 'cshoc': 'shrout', 'cshtrd': 'vol',
                                              'prccd': 'close', 'prchd': 'high', 'prcld': 'low', 'prcod': 'open'})
        comp_secd[date_col_name] = us.dt_to_str(comp_secd[date_col_name])

        comp_secd = comp_secd.merge(exch_rates,
                                    left_on=[date_col_name, 'curr'],
                                    right_on=[date_col_name, 'curr']).drop('curr', axis=1)
        comp_secd = comp_secd.dropna(subset=['ric', 'close'])
        comp_secd.to_csv(raw_output_dir + 'comp_secd.csv', index=False)
        print('comp_secd saved and loaded!')

        for p_type in ['open', 'high', 'low', 'close']:
            comp_secd.loc[:, f'{p_type}(GBP)'] = comp_secd.loc[:, p_type] / comp_secd.loc[:, 'GBPXXX']
            comp_secd.loc[:, f'{p_type}(GBP)'] = comp_secd.loc[:, f'{p_type}(GBP)'].round(2)
            comp_secd = comp_secd.drop(p_type, axis=1)

            p_df = comp_secd.loc[:, ['ric', date_col_name, f'{p_type}(GBP)']]
            p_df = pivot(p_df)
            p_df = us.fillna(p_df, hat_in_cols=True)
            p_df = p_df.replace(0, float('NaN'))
            if p_type == 'close':
                p_df = us.fillna(p_df, hat_in_cols=True)
            p_df.to_csv(preprocessed_dir + f'price/{p_type}.csv')
            print(f'{p_type} saved!')

        # shrout
        shrout_df = comp_secd.loc[:, ['ric', date_col_name, 'shrout']]
        shrout_df = pivot(shrout_df)
        shrout_df = us.fillna(shrout_df, hat_in_cols=True)
        shrout_df = shrout_df.replace(0, float('NaN'))
        shrout_df = us.fillna(shrout_df, hat_in_cols=True)
        shrout_df.to_csv(preprocessed_dir + 'price/shrout.csv')
        print('shrout saved!')

        # market value = market cap
        mve_df = p_df * shrout_df
        mve_df = us.fillna(mve_df, hat_in_cols=True)
        mve_df = mve_df.replace(0, float('NaN'))
        mve_df.to_csv(preprocessed_dir + 'price/mve.csv')
        print('mve saved!')

        # dividend saving
        comp_secd = comp_secd.drop('GBPXXX', axis=1)
        comp_secd = comp_secd.merge(exch_rates, left_on=[date_col_name, 'curr_div'], right_on=[date_col_name, 'curr'])
        comp_secd = comp_secd.drop(['curr', 'curr_div'], axis=1)
        comp_secd['divd(GBP)'] = comp_secd.loc[:, 'divd'] / comp_secd.loc[:, 'GBPXXX']
        comp_secd = comp_secd.drop(['GBPXXX', 'divd'], axis=1)

        divd_df = comp_secd[[date_col_name, 'ric', 'divd(GBP)']]
        divd_df = pivot(divd_df)
        divd_df = us.fillna(divd_df, hat_in_cols=True)
        divd_df.to_csv(preprocessed_dir + 'price/dividend.csv')
        print('dividend saved!')

        rics = divd_df.columns[1:]
    else:
        rics = pd.read_csv(preprocessed_dir + 'price/close.csv').columns[1:]
    ric1s = [ric.split('^')[0] for ric in rics]
    ric_dict = dict(zip(ric1s, rics))

    organise_fund = False
    if organise_fund:
        funds = ['q', '']
    else:
        funds = []
    for fsth in funds:
        comp_fund = pd.read_csv(useful_dir + f'comp_fund{"a" if fsth=="" else fsth}.csv', low_memory=False)
        for cols in ['conm', 'fyear', 'fyearq', 'fqtr']:
            try:
                comp_fund = comp_fund.drop(cols, axis=1)
            except KeyError:
                pass
        date_col_name = us.date_col_finder(comp_fund, f'comp_fund{"a" if fsth=="" else fsth}')
        comp_fund[date_col_name] = us.dt_to_str(comp_fund[date_col_name])
        exch_rates = exch_rates.loc[exch_rates[date_col_name] >= problematic_before, :].reset_index(drop=True)

        # gvkey -> ric
        comp_fund = comp_fund.sort_values(by=['gvkey', date_col_name])
        comp_fund = collapse_dup(comp_fund, subset=['gvkey', date_col_name],
                                 df_name=f'comp_fund{"a" if fsth=="" else fsth}')
        comp_fund[['gvkey', 'curcd' + fsth]] = comp_fund[['gvkey', 'curcd' + fsth]].groupby('gvkey').apply(
            lambda group: group['curcd' + fsth].replace(0, float('NaN')).fillna(method='ffill')).reset_index(level=0)
        comp_fund = comp_fund.dropna(subset='curcd' + fsth)

        comp_fund = gvkey2ric(comp_fund, gvkey_ric_dict) if mix_ref else gvkey2ric(comp_fund)

        comp_fund = comp_fund[comp_fund['ric'].isin(ric1s)]
        comp_fund = comp_fund.rename(columns={'curcd' + fsth: 'curr'})
        comp_fund['ric'] = comp_fund['ric'].map(dict(zip(ric1s, rics)))
        comp_fund[date_col_name] = us.dt_to_str(comp_fund[date_col_name])
        comp_fund = comp_fund.merge(exch_rates,
                                    left_on=[date_col_name, 'curr'],
                                    right_on=[date_col_name, 'curr']).drop('curr', axis=1)
        comp_fund['count' + fsth] = comp_fund.groupby('ric').cumcount() + 1  # line 76

        # comp_fund['dr'+fsth] = np.NAN  # TODO: line 86-92
        if 'xint' + fsth in comp_fund.columns:
            comp_fund.loc[:, 'xint' + fsth] = comp_fund.loc[:, 'xint' + fsth].fillna(0)  # line 94
        if 'xsga' + fsth in comp_fund.columns:
            comp_fund.loc[:, 'xsga' + fsth] = comp_fund.loc[:, 'xsga' + fsth].fillna(0)  # line 96

        comp_fund[date_col_name] = us.dt_to_str(comp_fund[date_col_name])

        to_export = comp_fund.columns
        for acol in ['ric', date_col_name, 'GBPXXX']:
            try:
                to_export = to_export.drop(acol)
            except KeyError:
                pass
        to_export = to_export.sort_values()
        for acol in to_export:
            new_acol = acol
            if acol != 'count'+fsth:
                new_acol = new_acol + '(GBP)'
                comp_fund.loc[:, new_acol] = comp_fund.loc[:, acol] / comp_fund.loc[:, 'GBPXXX']
                comp_fund = comp_fund.drop(acol, axis=1)
                comp_fund.loc[:, new_acol] = comp_fund.loc[:, new_acol].round(4)

            new_df = comp_fund.loc[:, ['ric', date_col_name, new_acol]]
            new_df = pivot(new_df)
            new_df = us.fillna(new_df, hat_in_cols=True)
            new_df.to_csv(preprocessed_dir + f'F{"Y" if fsth == "" else "Q"}/' + acol + '.csv')
            print(acol + f'{"a" if fsth == "" else fsth} saved!')

        comp_fund.to_csv(raw_output_dir + f'comp_fund{"a" if fsth == "" else fsth}.csv', index=False)
        print(f'comp_fund{"a" if fsth == "" else fsth} saved!')

    make_monthly = True
    if make_monthly:
        # producing first days of months
        close_df = pd.read_csv(preprocessed_dir + 'price/close.csv')
        date_col_name = us.date_col_finder(close_df, 'close.csv')
        close_df[date_col_name] = us.dt_to_str(close_df[date_col_name])
        date_vec = close_df[date_col_name].drop_duplicates().sort_values()
        date_vec = (date_vec[date_vec.str[5:7] != date_vec.str[5:7].shift(1)])

        from_dirs = [preprocessed_dir + subdir for subdir in ['FQ/', 'FY/', 'price/']]
        to_dirs = [processed_dir + 'input_' + subdir for subdir in ['fundq/', 'funda/', 'secd/']]

        for i in range(3):
            from_dir = from_dirs[i]
            to_dir = to_dirs[i]
            for filename in us.listdir(from_dir):
                df = pd.read_csv(from_dir + filename)
                df = df.set_index(us.date_col_finder(df, from_dir + filename))
                df = pd.concat([df, pd.DataFrame(float('NaN'),
                                                 index=date_vec[~date_vec.isin(df.index)],
                                                 columns=df.columns)])
                df = df.sort_index()
                df = us.fillna(df, hat_in_cols=True)
                df = df.dropna(axis=1, how='all')
                df = df.loc[date_vec, :]
                df.to_csv(to_dir + filename, index=True)
                print(filename + ' saved!')

