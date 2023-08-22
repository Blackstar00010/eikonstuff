import os
import Misc.useful_stuff as us
import pandas as pd

all_dir = '/Users/jamesd/Desktop/wrds_data/all/'
useful_dir = '/Users/jamesd/Desktop/wrds_data/useful/'
raw_output_dir = '../data/preprocessed_wrds/'
by_data_dir = '../data/processed_wrds/'


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
    Only for pivoting df with columns 'ric', 'datadate', data into df of columns of rics and rows of datadates
    :param some_df: dataframe to pivot
    :return: pivoted dataframe
    """
    value = some_df.columns.drop(['datadate', 'ric'])
    ret = some_df.pivot_table(index='datadate', columns='ric', values=value)
    ret.columns = ret.columns.droplevel(level=0)  # b/c of two-level column names
    return ret


if __name__ == "__main__":
    # gvkeys -> RIC dictionary
    ref_comp_table = pd.read_csv('../data/metadata/ref-comp.csv')[['ric', 'gvkey']]
    gvkey_ric_dict = {}
    for i in range(len(ref_comp_table)):
        gvkey_ric_dict[ref_comp_table.loc[i, 'gvkey']] = ref_comp_table.loc[i, 'ric']

    # GBPXXX
    exch_rates = pd.read_csv(all_dir + 'comp_exrt_all.csv', low_memory=False).loc[:, ['tocurd', 'exratd', 'datadate']]
    exch_rates = exch_rates.rename(columns={'tocurd': 'curr', 'exratd': 'GBPXXX'})
    # changing date format
    date_col_name = us.date_col_finder(exch_rates, 'exch_rates')
    exch_rates[date_col_name] = us.datetime_to_str(exch_rates[date_col_name].astype(str))

    organize_secd = False
    if organize_secd:
        # gvkey,datadate,conm,isin,exchg,monthend,curcdd,ajexdi,cshoc,cshtrd,prccd,prchd,prcld,prcod,curcddv,divd,paydate,split
        comp_secd = pd.read_csv(useful_dir + 'comp_secd.csv', low_memory=False)
        comp_secd = comp_secd.drop(['conm', 'isin', 'exchg'], axis=1)
        comp_secd['curcdd'] = us.fillna(comp_secd['curcdd'])  # todo: polluting other companies?
        comp_secd = comp_secd.dropna(subset='curcdd')

        # changing date format
        date_col_name = us.date_col_finder(comp_secd, 'comp_secd')
        comp_secd.loc[:, date_col_name] = us.datetime_to_str(comp_secd.loc[:, date_col_name])

        # gvkey -> ric
        comp_secd = comp_secd[comp_secd['gvkey'].isin(list(gvkey_ric_dict.keys()))]
        comp_secd.loc[:, 'ric'] = comp_secd.loc[:, 'gvkey'].map(gvkey_ric_dict)
        comp_secd = comp_secd.drop('gvkey', axis=1)

        comp_secd = comp_secd.rename(columns={'curcdd': 'curr', 'curcddv': 'curr_div',
                                              'ajexdi': 'adj', 'cshoc': 'shrout', 'cshtrd': 'vol',
                                              'prccd': 'close', 'prchd': 'high', 'prcld': 'low', 'prcod': 'open'})
        comp_secd['datadate'] = us.datetime_to_str(comp_secd['datadate'])
        comp_secd = comp_secd.merge(exch_rates,
                                    left_on=['datadate', 'curr'], right_on=['datadate', 'curr']).drop('curr', axis=1)
        comp_secd = comp_secd.dropna(subset=['ric', 'close'])
        print('comp_secd loaded!')
        for p_type in ['open', 'high', 'low', 'close']:
            comp_secd.loc[:, f'{p_type}(GBP)'] = comp_secd.loc[:, p_type] / comp_secd.loc[:, 'GBPXXX']
            comp_secd.loc[:, f'{p_type}(GBP)'] = comp_secd.loc[:, f'{p_type}(GBP)'].round(2)
            comp_secd = comp_secd.drop(p_type, axis=1)

            p_df = comp_secd.loc[:, ['ric', 'datadate', f'{p_type}(GBP)']]
            p_df = pivot(p_df)
            p_df = us.fillna(p_df)
            p_df = p_df.replace(0, float('NaN'))
            p_df.to_csv(by_data_dir + f'input_secd/{p_type}.csv')
            print(f'{p_type} saved!')

        # shrout
        shrout_df = comp_secd.loc[:, ['ric', 'datadate', 'shrout']]
        shrout_df = pivot(shrout_df)
        shrout_df = us.fillna(shrout_df)
        shrout_df = shrout_df.replace(0, float('NaN'))
        shrout_df.to_csv(by_data_dir + 'input_secd/shrout.csv')
        print('shrout saved!')

        # market value = market cap
        mve_df = p_df * shrout_df
        mve_df = us.fillna(mve_df)
        mve_df = mve_df.replace(0, float('NaN'))
        mve_df.to_csv(by_data_dir + 'input_secd/mve.csv')
        print('mve saved!')

        # dividend saving
        comp_secd = comp_secd.drop('GBPXXX', axis=1)
        comp_secd = comp_secd.merge(exch_rates, left_on=['datadate', 'curr_div'], right_on=['datadate', 'curr'])
        comp_secd = comp_secd.drop(['curr', 'curr_div'], axis=1)
        comp_secd['divd(GBP)'] = comp_secd.loc[:, 'divd'] / comp_secd.loc[:, 'GBPXXX']
        comp_secd = comp_secd.drop(['GBPXXX', 'divd'], axis=1)

        divd_df = comp_secd[['datadate', 'ric', 'divd(GBP)']]
        divd_df = pivot(divd_df)
        divd_df = us.fillna(divd_df)
        divd_df.to_csv(by_data_dir + 'input_secd/dividend.csv')
        print('dividend saved!')

        comp_secd.to_csv(raw_output_dir + 'comp_secd.csv', index=False)
        print('comp_secd saved!')

        rics = divd_df.columns[1:]
    else:
        rics = pd.read_csv(by_data_dir + 'input_secd/close.csv').columns[1:]

    organize_funda = False
    if organize_funda:
        # gvkey,conm,datadate,fyear,curcd,
        # sale,revt,cogs,xsga,dp,xrd,ib,ebitda,ebit,nopi,spi,pi,txp,txt,xint,
        # capx,oancf,dvt,
        # rect,act,che,ppegt,invt,at,aco,intan,ao,ppent,gdwl,fatb,fatl,
        # lct,dlc,dltt,lt,pstk,ap,lco,lo,txdi,
        # ceq,emp
        comp_funda = pd.read_csv(useful_dir + 'comp_funda.csv', low_memory=False)
        comp_funda = comp_funda.drop(['conm', 'fyear'], axis=1)
        comp_funda['curcd'] = us.fillna(comp_funda['curcd'])
        comp_funda = comp_funda.dropna(subset='curcd')

        # gvkey -> ric
        comp_funda = comp_funda[comp_funda['gvkey'].isin(list(gvkey_ric_dict.keys()))]
        comp_funda['ric'] = comp_funda['gvkey'].map(gvkey_ric_dict)
        comp_funda = comp_funda.drop('gvkey', axis=1)

        comp_funda = comp_funda[comp_funda['ric'].isin(rics)]
        comp_funda = comp_funda.rename(columns={'curcd': 'curr'})
        comp_funda['datadate'] = us.datetime_to_str(comp_funda['datadate'])
        comp_funda = comp_funda.merge(exch_rates,
                                      left_on=['datadate', 'curr'],
                                      right_on=['datadate', 'curr']).drop('curr', axis=1)
        comp_funda['count'] = comp_funda.groupby('ric').cumcount() + 1  # line 76

        # comp_funda['dr'] = np.NAN  # TODO: line 86-92
        comp_funda.loc[:, 'xint'] = comp_funda.loc[:, 'xint'].fillna(0)  # line 94
        comp_funda.loc[:, 'xsga'] = comp_funda.loc[:, 'xsga'].fillna(0)  # line 96

        comp_funda[us.date_col_finder(comp_funda, 'comp_funda')] = us.datetime_to_str(
            comp_funda[us.date_col_finder(comp_funda, 'comp_funda')])

        to_export = comp_funda.columns
        for acol in ['ric', 'datadate', 'GBPXXX']:
            try:
                to_export = to_export.drop(acol)
            except KeyError:
                pass
        for acol in to_export:
            new_acol = acol
            if acol != 'count':
                new_acol = new_acol + '(GBP)'
                comp_funda.loc[:, new_acol] = comp_funda.loc[:, acol] / comp_funda.loc[:, 'GBPXXX']
                comp_funda = comp_funda.drop(acol, axis=1)
                comp_funda.loc[:, new_acol] = comp_funda.loc[:, new_acol].round(4)

            new_df = comp_funda.loc[:, ['ric', 'datadate', new_acol]]
            new_df = pivot(new_df)
            new_df.to_csv(by_data_dir + 'input_funda/' + acol + '.csv')
            print(acol + ' saved!')

        comp_funda.to_csv(raw_output_dir + 'comp_funda.csv', index=False)
        print('comp_funda saved!')

    organize_fundq = False
    if organize_fundq:
        # gvkey,fyearq,fqtr,datadate,curcdq,
        # ibq,saleq,txtq,revtq,cogsq,xsgaq,atq,actq,cheq,lctq,dlcq,ppentq,ceqq,seqq,ltq
        comp_fundq = pd.read_csv(useful_dir + 'comp_fundq.csv', low_memory=False)
        comp_fundq = comp_fundq.drop(['fyearq', 'fqtr'], axis=1)
        comp_fundq = comp_fundq.dropna(subset='curcdq')

        # gvkey -> ric
        comp_fundq = comp_fundq[comp_fundq['gvkey'].isin(list(gvkey_ric_dict.keys()))]
        comp_fundq['ric'] = comp_fundq['gvkey'].map(gvkey_ric_dict)
        comp_fundq = comp_fundq.drop('gvkey', axis=1)

        comp_fundq = comp_fundq[comp_fundq['ric'].isin(rics)]
        comp_fundq = comp_fundq.rename(columns={'curcdq': 'curr'})
        comp_fundq['datadate'] = us.datetime_to_str(comp_fundq['datadate'])
        comp_fundq = comp_fundq.merge(exch_rates, left_on=['datadate', 'curr'], right_on=['datadate', 'curr']).drop(
            'curr', axis=1)

        comp_fundq[us.date_col_finder(comp_fundq, 'comp_fundq')] = us.datetime_to_str(
            comp_fundq[us.date_col_finder(comp_fundq, 'comp_fundq')])

        to_export = comp_fundq.columns
        for acol in ['ric', 'datadate', 'GBPXXX']:
            try:
                to_export = to_export.drop(acol)
            except KeyError:
                pass
        for acol in to_export:
            new_acol = acol
            if acol != 'count':
                new_acol = new_acol + '(GBP)'
                comp_fundq.loc[:, new_acol] = comp_fundq.loc[:, acol] / comp_fundq.loc[:, 'GBPXXX']
                comp_fundq = comp_fundq.drop(acol, axis=1)
                comp_fundq.loc[:, new_acol] = comp_fundq.loc[:, new_acol].round(4)

            new_df = comp_fundq.loc[:, ['ric', 'datadate', new_acol]]
            new_df = pivot(new_df)
            new_df.to_csv(by_data_dir + 'input_fundq/' + acol + '.csv')
            print(acol + ' saved!')

        comp_fundq.to_csv(raw_output_dir + 'comp_fundq.csv', index=False)
        print('comp_fundq saved!')

    apply_good_date = True
    if apply_good_date:
        target_dirs = ['../data/processed_wrds/'+ item for item in ['input_funda', 'input_fundq', 'input_secd']]
        date_df = pd.read_csv('../data/metadata/business_days.csv')[['YYYY-MM-DD', 'YYYY']]
        for adir in target_dirs:
            files = us.listdir(adir)
            for afile in files:
                df = pd.read_csv(adir + '/' + afile)
                df = df.merge(date_df, left_on=us.date_col_finder(df, afile), right_on='YYYY-MM-DD', how='outer')
                df['datadate'] = df['datadate'].fillna(df['YYYY-MM-DD'])
                df = df.drop(columns=['YYYY-MM-DD', 'YYYY'])
                df = df.sort_index()
                if 'secd' in adir:
                    df = us.fillna(df)
                else:
                    df = df.fillna(method='ffill')
                for ashit in ['Unnamed: 0', 'Unnamed: 0.1']:
                    if ashit in df.columns:
                        df = df.drop(ashit, axis=1)
                df.to_csv(adir + '/' + afile, index=False)
                print(f'{afile} done!')
            print(f'{adir} done!')
