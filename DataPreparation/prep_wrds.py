import numpy as np
import DataComputation._options as opt
import Misc.useful_stuff as us
import pandas as pd
import multiprocessing as mp
from spec_cases import fix_secd

all_dir = '/Users/jamesd/Desktop/wrds_data/all/'
useful_dir = '/Users/jamesd/Desktop/wrds_data/useful/'
raw_output_dir = opt.raw_dir
preprocessed_dir = opt.preprocessed_dir
processed_dir = opt.processed_dir


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


def gvkey2ric(some_df: pd.DataFrame, gvkey_ric_dict: dict = None, add_hat: bool = False,
              drop: bool = False, isin_col=None, sedol_col=None) -> pd.DataFrame:
    """
    Add ric column named 'ric' to dataframe based on gvkey column. If ``gvkey_ric_dict`` is None, use us.num2ric function
    :param some_df: dataframe with gvkey column
    :param gvkey_ric_dict: dictionary of {gvkey1: ric1, ...}
    :param add_hat: whether to add hat and last valid data's date to ric
    :param drop: whether to drop gvkey, isin, sedol column
    :return: dataframe with gvkey column replaced by ric column
    """
    if gvkey_ric_dict is not None:
        # i.e., joining with refinitiv data or some other prep work is done
        some_df = some_df[some_df['gvkey'].isin(list(gvkey_ric_dict.keys()))]
        some_df.loc[:, 'ric'] = some_df.loc[:, 'gvkey'].map(gvkey_ric_dict)
        if drop:
            some_df = some_df.drop('gvkey', axis=1)
    else:
        some_df.loc[:, 'ric'] = some_df.loc[:, 'gvkey'].apply(us.num2ric)
        if drop:
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

    # adding suffix to manage single-ric-multiple-isin problem
    some_df['ric_suffix'] = float('NaN')
    if isin_col is not None:
        some_df['ric_suffix'] = some_df['ric_suffix'].fillna(some_df[isin_col].str[-3:-1])
        some_df = some_df.drop(isin_col, axis=1) if drop else some_df
    if sedol_col is not None:
        some_df['ric_suffix'] = some_df['ric_suffix'].fillna(some_df[sedol_col].str[-2:])
        some_df = some_df.drop(sedol_col, axis=1) if drop else some_df
    some_df['ric_suffix'] = '_' + some_df['ric_suffix']
    some_df['ric_suffix'] = some_df['ric_suffix'].replace('_', float('NaN'))
    some_df['ric'] = (some_df['ric'].str.split('-').str[0] +
                      some_df['ric_suffix'] + '-' +
                      some_df['ric'].str.split('-').str[-1])
    some_df = some_df.drop('ric_suffix', axis=1)

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
        subset = some_df.columns.tolist()
    some_df = some_df.sort_values(by=subset).reset_index(drop=True)
    dirty_index = some_df.index[some_df.duplicated(subset, keep=False)]
    dirty_part = some_df.loc[dirty_index, :]
    ret = some_df.drop(dirty_index, axis=0)

    close_col_name = ''
    for candidates in ['prccd', 'close', 'Close']:
        if candidates in some_df.columns.tolist():
            close_col_name = candidates
            break
    vol_col_name = ''
    for candidates in ['cshtrd', 'vol', 'Volume']:
        if candidates in some_df.columns.tolist():
            vol_col_name = candidates
            break

    try:
        sort_by = subset.copy()
        sort_by.append(vol_col_name)
        drop_by = sort_by.copy()
        drop_by.append(close_col_name)
        dirty_part = dirty_part.dropna(subset=drop_by, how='any')
        dirty_part = dirty_part.sort_values(by=sort_by).reset_index(drop=True)
    except KeyError:
        dirty_part = dirty_part.sort_values(by=subset).reset_index(drop=True)

    dirty_part['group_no'] = dirty_part.groupby(subset).ngroup()
    cleaner_dirty_part = dirty_part.drop_duplicates(subset=subset, keep='last')
    try:
        cleaner_dirty_part = cleaner_dirty_part.dropna(subset=close_col_name)
        dirtier_dirty_part = dirty_part[~dirty_part['group_no'].isin(cleaner_dirty_part['group_no'])]
        cleaner_dirty_part = cleaner_dirty_part.drop('group_no', axis=1)
    except KeyError:  # because close_col_name is ''
        cleaner_dirty_part = pd.DataFrame([])
        dirtier_dirty_part = dirty_part

    dirtier_dirty_part = dirtier_dirty_part.groupby('group_no').apply(
        lambda group: group.fillna(method='ffill')).reset_index(drop=True)
    dirtier_dirty_part = dirtier_dirty_part.drop_duplicates(subset=subset, keep='last').drop('group_no', axis=1)
    dirtier_dirty_part[close_col_name] = float('NaN')
    ret = pd.concat([ret, cleaner_dirty_part, dirtier_dirty_part], axis=0).sort_values(by=subset).reset_index(drop=True)
    if df_name is not None:
        print(f'Finished managing duplicates of {df_name} data!')
    return ret


if __name__ == "__main__":
    # metadata
    mix_ref = False
    problematic_before = '1985-01-01'  # only data from this date

    if mix_ref:
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

    organize_secd = True
    if organize_secd:
        # gvkey,datadate,isin,sedol,exchg,monthend,curcdd,ajexdi,cshoc,cshtrd,qunit,
        # prccd,prchd,prcld,prcod,curcddv,divd,paydate,split
        comp_secd = pd.read_csv(useful_dir + 'comp_secd.csv', low_memory=False)
        comp_secd = comp_secd.drop(['exchg'], axis=1)
        comp_secd = comp_secd.rename(columns={'curcdd': 'curr', 'curcddv': 'curr_div',
                                              'ajexdi': 'adj', 'cshoc': 'shrout', 'cshtrd': 'vol',
                                              'prccd': 'close', 'prchd': 'high', 'prcld': 'low', 'prcod': 'open'})

        # order: dateformat -> gvkey2ric -> collapse_dup -> groupffill -> currency

        # change date format
        date_col_name = us.date_col_finder(comp_secd, 'comp_secd')
        comp_secd.loc[:, date_col_name] = us.dt_to_str(comp_secd.loc[:, date_col_name])
        comp_secd = comp_secd.loc[comp_secd[date_col_name] >= problematic_before, :].reset_index(drop=True)

        # manage special cases (should be done before gvkey2ric)
        comp_secd = fix_secd(comp_secd)

        # only the most frequent isin for each gvkey
        comp_secd['isin'] = comp_secd['isin'].fillna(comp_secd['sedol']).fillna('JD')
        most_frequent_isin = comp_secd.groupby('gvkey')['isin'].agg(lambda x: x.value_counts().idxmax())
        comp_secd = comp_secd.merge(most_frequent_isin.reset_index(), on='gvkey')
        comp_secd = comp_secd[comp_secd['isin_x'] == comp_secd['isin_y']]
        comp_secd = comp_secd.drop(['isin_y'], axis=1).rename(columns={'isin_x': 'isin'})

        # gvkey -> ric, reformatting
        if mix_ref:
            comp_secd = gvkey2ric(comp_secd, gvkey_ric_dict)
        else:
            comp_secd = gvkey2ric(comp_secd, add_hat=True, drop=True, isin_col='isin', sedol_col='sedol')

        # remove duplicates
        comp_secd = collapse_dup(comp_secd, subset=['ric', date_col_name], df_name='comp_secd')

        # group ffill & price adjustment
        ohlc_list = ['open', 'high', 'low', 'close']
        to_ffill = us.flatten([ohlc_list, 'vol', 'shrout', 'adj'])
        comp_secd[us.flatten(['ric', to_ffill])] = comp_secd[us.flatten(['ric', to_ffill])].groupby('ric').apply(
            lambda group: group[to_ffill].fillna(method='ffill')
        ).reset_index(level=0)
        comp_secd[ohlc_list] = comp_secd[ohlc_list].multiply(1 / comp_secd['qunit'].replace(float('NaN'), 1),
                                                             axis=0)
        comp_secd[ohlc_list] = comp_secd[ohlc_list].multiply(1 / comp_secd['adj'].replace(float('NaN'), 1),
                                                             axis=0)
        comp_secd = comp_secd.dropna(subset=['curr', 'close'], how='any')
        comp_secd = comp_secd.merge(exch_rates,
                                    left_on=[date_col_name, 'curr'],
                                    right_on=[date_col_name, 'curr']).drop('curr', axis=1)

        comp_secd.to_csv(raw_output_dir + 'comp_secd.csv', index=False)
        print('comp_secd saved and loaded!')

        # save by ric
        # for aric in comp_secd['ric'].unique():
        #     df = comp_secd[comp_secd['ric'] == aric]
        #     df = df.sort_values(by=[date_col_name])
        #     df.to_csv(preprocessed_dir + f'price/by_ric/{aric}.csv', index=False)
        # us.beep()

        # for p_type in ['high', 'low', 'open', 'close']:
        for p_type in ['close']:
            comp_secd.loc[:, f'{p_type}(GBP)'] = comp_secd.loc[:, p_type] / comp_secd.loc[:, 'GBPXXX']
            comp_secd.loc[:, f'{p_type}(GBP)'] = comp_secd.loc[:, f'{p_type}(GBP)'].round(2)
            comp_secd = comp_secd.drop(p_type, axis=1)

            p_df = comp_secd.loc[:, ['ric', date_col_name, f'{p_type}(GBP)']]
            p_df = pivot(p_df)
            p_df = us.fillna(p_df, hat_in_cols=True)
            p_df = p_df.replace(0, float('NaN'))
            p_df.to_csv(preprocessed_dir + f'price/{p_type}.csv')
            print(f'{p_type} saved!')

        # shrout
        shrout_df = comp_secd.loc[:, ['ric', date_col_name, 'shrout']]
        shrout_df = pivot(shrout_df)
        shrout_df = shrout_df.replace(0, float('NaN'))
        shrout_df = us.fillna(shrout_df, hat_in_cols=True)
        shrout_df.to_csv(preprocessed_dir + 'price/shrout.csv')
        print('shrout saved!')

        # market value = market cap
        mve_df = pd.read_csv(preprocessed_dir + 'price/close.csv')
        mve_df = mve_df.set_index(us.date_col_finder(mve_df, 'close')) * shrout_df
        mve_df = mve_df * pivot(comp_secd.loc[:, ['ric', date_col_name, 'adj']])
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
        funds = ['', 'q']
    else:
        funds = []
    for fsth in funds:
        comp_fund = pd.read_csv(useful_dir + f'comp_fund{"a" if fsth == "" else fsth}.csv', low_memory=False)
        for cols in ['conm', 'fyear', 'fyearq', 'fqtr']:
            try:
                comp_fund = comp_fund.drop(cols, axis=1)
            except KeyError:
                pass

        # order: dateformat -> gvkey2ric -> rename -> collapse_dup -> groupffill -> currency

        # change date format
        comp_fund = us.change_date_format(comp_fund, 'comp_fund' + f'{"a" if fsth == "" else fsth}')
        exch_rates = exch_rates.loc[exch_rates[date_col_name] >= problematic_before, :].reset_index(drop=True)

        # gvkey -> ric, reformatting
        if mix_ref:
            comp_fund = gvkey2ric(comp_fund, gvkey_ric_dict)
        else:
            comp_fund = gvkey2ric(comp_fund, add_hat=False, drop=True, isin_col='isin', sedol_col='sedol')
        comp_fund = comp_fund[comp_fund['ric'].isin(ric1s)]
        comp_fund = comp_fund.rename(columns={'curcd' + fsth: 'curr'})
        comp_fund['ric'] = comp_fund['ric'].map(ric_dict)
        comp_fund = comp_fund.rename(columns={'curcd' + fsth: 'curr'})

        # remove duplicates
        comp_fund = collapse_dup(comp_fund, subset=['ric', date_col_name],
                                 df_name=f'comp_fund{"a" if fsth == "" else fsth}')

        # group ffill & currency matching
        comp_fund[['ric', 'curr']] = comp_fund[['ric', 'curr']].groupby('ric').apply(
            lambda group: group['curr'].replace(0, float('NaN')).fillna(method='ffill')).reset_index(level=0)
        comp_fund = comp_fund.dropna(subset='curr')
        comp_fund = comp_fund.merge(exch_rates,
                                    left_on=[date_col_name, 'curr'],
                                    right_on=[date_col_name, 'curr']).drop('curr', axis=1)

        comp_fund['count' + fsth] = comp_fund.groupby('ric').cumcount() + 1  # line 76

        # other variables
        # comp_fund['dr'+fsth] = np.NAN  # TODO: line 86-92
        if 'xint' + fsth in comp_fund.columns:
            comp_fund.loc[:, 'xint' + fsth] = comp_fund.loc[:, 'xint' + fsth].fillna(0)  # line 94
        if 'xsga' + fsth in comp_fund.columns:
            comp_fund.loc[:, 'xsga' + fsth] = comp_fund.loc[:, 'xsga' + fsth].fillna(0)  # line 96

        to_export = comp_fund.columns
        for acol in ['ric', date_col_name, 'GBPXXX', '', 'curr']:
            try:
                to_export = to_export.drop(acol)
            except KeyError:
                pass
        to_export = to_export.sort_values()
        for acol in to_export:
            new_acol = acol
            if acol != 'count' + fsth:
                new_acol = new_acol + '(GBP)'
                comp_fund.loc[:, new_acol] = comp_fund.loc[:, acol] / comp_fund.loc[:, 'GBPXXX']
                comp_fund = comp_fund.drop(acol, axis=1)
                comp_fund.loc[:, new_acol] = comp_fund.loc[:, new_acol].round(4)

            new_df = comp_fund.loc[:, ['ric', date_col_name, new_acol]]
            new_df = pivot(new_df)
            new_df = us.fillna(new_df, hat_in_cols=True)
            new_df.to_csv(preprocessed_dir + f'F{"Y" if fsth == "" else "Q"}/' + acol + '.csv')
            print(acol + ' saved!')

        comp_fund.to_csv(raw_output_dir + f'comp_fund{"a" if fsth == "" else fsth}.csv', index=False)
        print(f'comp_fund{"a" if fsth == "" else fsth} saved!')

    organise_ibes = False
    if organise_ibes:
        for period in ['ann', 'qtr']:
            ibes_df = pd.read_csv(all_dir + f'ibes_{period}_est_all.csv', low_memory=False)
            # todo: manage cusip

    organise_cred = False
    if organise_cred:
        cred_df = pd.read_csv(useful_dir + 'comp_adsprate.csv', low_memory=False)
        date_col_name = us.date_col_finder(cred_df, 'comp_adsprate')
        cred_df = gvkey2ric(cred_df)

        test_dir = '../data/processed_wrds/output_by_month/'
        for afile in us.listdir(test_dir):
            test_df = pd.read_csv(test_dir + afile)
            ric1s = test_df.iloc[:, 0].str.split('^').apply(lambda x: x[0])
            print(afile, cred_df.loc[cred_df.index[cred_df['ric'].isin(ric1s)], 'ric'].nunique(), '/', len(ric1s))

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

        for i in range(len(from_dirs)-3):
            from_dir = from_dirs[i]
            to_dir = to_dirs[i]
            for filename in us.listdir(from_dir):
                if filename == '':
                    continue
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

        monthly_return_df = close_df.set_index(date_col_name)
        monthly_return_df = monthly_return_df.pct_change()
        monthly_return_df.to_csv(opt.output_dir + 'monthly_return.csv', index=True)
