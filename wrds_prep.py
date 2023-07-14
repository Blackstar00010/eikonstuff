import os

import numpy as np
import pandas as pd

all_dir = '/Users/jamesd/Desktop/data/all/'
useful_dir = '/Users/jamesd/Desktop/data/useful/'
raw_output_dir = 'files/wrds/'
by_data_dir = 'files/by_data/'

"""
0_comp_ref_tl_table.csv
comp_adsprate.csv
comp_company.csv
comp_exrt_all.csv
comp_funda.csv
comp_fundq.csv
comp_secd.csv
ibes_ann_est.csv
ibes_qtr_est.csv
"""

""" USE WHEN YOU ARE USING DATA FROM REFINITIV
df1 = pd.read_csv(dt_wrds_dir+'comp_names1_all.csv')
df2 = pd.read_csv(dt_wrds_dir+'comp_names2_all.csv')
df = pd.concat([df1, df2], ignore_index=True)
df = df.sort_values('gvkey').reset_index()
df = df.drop('index', axis=1)
dup = df[['gvkey', 'isin']].duplicated()
df = df[~dup]
df.to_csv(dt_wrds_dir+'comp_names_all.csv', index=False)

if input("DID YOU RUN tl_table_generator.py?[y/n]") != 'y':
    import sys
    sys.exit()

files = os.listdir('files/wrds/')
files.pop(files.index('0_comp_ref_tl_table.csv'))
for file in files:
    tl_table = pd.read_csv('files/wrds/0_comp_ref_tl_table.csv')[['gvkey', 'ric', 'isin']]
    funda = pd.read_csv('files/wrds/'+file)
    funda = pd.merge(funda, tl_table, on='gvkey', how='inner')
    funda = funda[funda['isin'].notna()]
    funda = funda[funda.columns.to_list()[-2:] + funda.columns.to_list()[:-2]]
    funda.to_csv('files/by_data/from_comp/'+file)
    print(file+' finished!')
"""


def shitty_pivot(some_df: pd.DataFrame):
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


def pivot(some_df: pd.DataFrame):
    """
    Only for pivoting df with columns 'gvkey', 'datadate', data into df of columns of gvkeys and rows of datadates
    :param some_df: dataframe to pivot
    :return: pivoted dataframe
    """
    value = some_df.columns.drop(['datadate', 'gvkey'])
    ret = some_df.pivot_table(index='datadate', columns='gvkey', values=value)
    ret.columns = ret.columns.droplevel(level=0)
    return ret


# GBPXXX
exch_rates = pd.read_csv(all_dir + 'comp_exrt_all.csv', low_memory=False).loc[:, ['tocurd', 'exratd', 'datadate']]
exch_rates = exch_rates.rename(columns={'tocurd': 'curr', 'exratd': 'GBPXXX'})

organize_secd = False
if organize_secd:
    # gvkey,datadate,conm,isin,exchg,monthend,curcdd,ajexdi,cshoc,cshtrd,prccd,prchd,prcld,prcod,curcddv,divd,paydate,split
    comp_secd = pd.read_csv(useful_dir + 'comp_secd.csv', low_memory=False)
    comp_secd = comp_secd.drop(['conm', 'isin', 'exchg'], axis=1)
    comp_secd = comp_secd.dropna(subset='curcdd')
    comp_secd = comp_secd.rename(columns={'curcdd': 'curr', 'curcddv': 'curr_div',
                                          'ajexdi': 'adj', 'cshoc': 'shrout', 'cshtrd': 'vol',
                                          'prccd': 'close', 'prchd': 'high', 'prcld': 'low', 'prcod': 'open'})
    comp_secd = comp_secd.merge(exch_rates, left_on=['datadate', 'curr'], right_on=['datadate', 'curr']).drop('curr',
                                                                                                              axis=1)
    # comp_secd[['shrout', 'vol', 'paydate']] = comp_secd[['shrout', 'vol', 'paydate']].astype(int)

    ohlc = []
    for p_type in ['open', 'high', 'low', 'close']:
        comp_secd.loc[:, f'{p_type}(GBP)'] = comp_secd.loc[:, p_type] / comp_secd.loc[:, 'GBPXXX']
        comp_secd.loc[:, f'{p_type}(GBP)'] = comp_secd.loc[:, f'{p_type}(GBP)'].round(2)
        ohlc.append(comp_secd.loc[:, ['gvkey', 'datadate', f'{p_type}(GBP)']])
        comp_secd = comp_secd.drop(p_type, axis=1)

    mve_df = ohlc[-1]
    mve_df = mve_df.merge(comp_secd.loc[:, ['gvkey', 'datadate', 'shrout']], left_on=['gvkey', 'datadate'],
                          right_on=['gvkey', 'datadate'])
    mve_df.loc[:, 'mve'] = mve_df.loc[:, 'close(GBP)'] * mve_df.loc[:, 'shrout']
    mve_df = mve_df.drop(['close(GBP)', 'shrout'], axis=1)
    mve_df = pivot(mve_df)
    mve_df.to_csv(by_data_dir + 'secd/mve.csv')
    print('mve saved!')

    for i, p_type in enumerate(['open', 'high', 'low', 'close']):
        p_df = ohlc[i]
        p_df = pivot(p_df)
        p_df.to_csv(by_data_dir + f'secd/{p_type}.csv')
        print(f'{p_type} saved!')

    comp_secd = comp_secd.drop('GBPXXX', axis=1)
    comp_secd = comp_secd.merge(exch_rates, left_on=['datadate', 'curr_div'],
                                right_on=['datadate', 'curr']).drop(['curr', 'curr_div'], axis=1)
    comp_secd['divd(GBP)'] = comp_secd.loc[:, 'divd'] / comp_secd.loc[:, 'GBPXXX']
    comp_secd = comp_secd.drop(['GBPXXX', 'divd'], axis=1)

    divd_df = comp_secd[['datadate', 'gvkey', 'divd(GBP)']]
    divd_df = pivot(divd_df)
    divd_df.to_csv(by_data_dir + 'secd/dividend.csv')
    print('dividend saved!')

    comp_secd.to_csv(raw_output_dir + 'comp_secd.csv', index=False)
    print('comp_secd saved!')

    gvkeys = divd_df.columns[1:]
else:
    gvkeys = pd.read_csv(by_data_dir + 'secd/close.csv').columns[1:]

organize_funda = True
if organize_funda:
    # gvkey,conm,datadate,fyear,curcd,
    # sale,revt,cogs,xsga,dp,xrd,ib,ebitda,ebit,nopi,spi,pi,txp,txt,xint,
    # capx,oancf,dvt,
    # rect,act,che,ppegt,invt,at,aco,intan,ao,ppent,gdwl,fatb,fatl,
    # lct,dlc,dltt,lt,pstk,ap,lco,lo,txdi,
    # ceq,emp
    comp_funda = pd.read_csv(useful_dir + 'comp_funda.csv', low_memory=False)
    comp_funda = comp_funda.drop(['conm', 'fyear'], axis=1)
    comp_funda = comp_funda.dropna(subset='curcd')
    comp_funda = comp_funda[comp_funda['gvkey'].isin(gvkeys)]
    comp_funda = comp_funda.rename(columns={'curcd': 'curr'})
    comp_funda = comp_funda.merge(exch_rates, left_on=['datadate', 'curr'], right_on=['datadate', 'curr']).drop('curr',
                                                                                                                axis=1)
    comp_funda['count'] = comp_funda.groupby('gvkey').cumcount() + 1    # line 76

    comp_funda['dr'] = np.NAN  # TODO: line 86-92
    comp_funda[:, 'xint0'] = comp_funda[:, 'xint'].fillna(0)  # line 94
    comp_funda = comp_funda.drop('xint', axis=1)
    comp_funda[:, 'xsga0'] = comp_funda[:, 'xsga'].fillna(0)  # line 96

    # not gvkey, datadate; not GBPXXX
    to_export = comp_funda.columns[2:-2]
    for acol in to_export:
        new_acol = acol + '(GBP)'
        comp_funda.loc[:, new_acol] = comp_funda.loc[:, acol] / comp_funda.loc[:, 'GBPXXX']
        comp_funda = comp_funda.drop(acol, axis=1)
        comp_funda.loc[:, new_acol] = comp_funda.loc[:, new_acol].round(4)

        new_df = comp_funda.loc[:, ['gvkey', 'datadate', new_acol]]
        new_df = pivot(new_df)
        new_df.to_csv(by_data_dir + 'funda/' + acol + '.csv')
        print(acol+' saved!')

    comp_funda.to_csv(raw_output_dir + 'comp_funda.csv', index=False)
    print('comp_funda saved!')

organize_fundq = True
if organize_fundq:
    # gvkey,fyearq,fqtr,datadate,curcdq,
    # ibq,saleq,txtq,revtq,cogsq,xsgaq,atq,actq,cheq,lctq,dlcq,ppentq,ceqq,seqq,ltq
    comp_fundq = pd.read_csv(useful_dir + 'comp_fundq.csv', low_memory=False)
    comp_fundq = comp_fundq.drop(['fyearq', 'fqtr'], axis=1)
    comp_fundq = comp_fundq.dropna(subset='curcdq')
    comp_fundq = comp_fundq[comp_fundq['gvkey'].isin(gvkeys)]
    comp_fundq = comp_fundq.rename(columns={'curcdq': 'curr'})
    comp_fundq = comp_fundq.merge(exch_rates, left_on=['datadate', 'curr'], right_on=['datadate', 'curr']).drop('curr',
                                                                                                                axis=1)

    # not gvkey, datadate; not GBPXXX
    to_export = comp_fundq.columns[2:-2]
    for acol in to_export:
        new_acol = acol + '(GBP)'
        comp_fundq.loc[:, new_acol] = comp_fundq.loc[:, acol] / comp_fundq.loc[:, 'GBPXXX']
        comp_fundq = comp_fundq.drop(acol, axis=1)
        comp_fundq.loc[:, new_acol] = comp_fundq.loc[:, new_acol].round(4)

        new_df = comp_fundq.loc[:, ['gvkey', 'datadate', new_acol]]
        new_df = pivot(new_df)
        new_df.to_csv(by_data_dir + 'fundq/' + acol + '.csv')
        print(acol+' saved!')

    comp_fundq.to_csv(raw_output_dir + 'comp_fundq.csv', index=False)
    print('comp_fundq saved!')

organize_ibesa = True
if organize_ibesa:
    # gvkey,fyearq,fqtr,datadate,curcdq,
    # ibq,saleq,txtq,revtq,cogsq,xsgaq,atq,actq,cheq,lctq,dlcq,ppentq,ceqq,seqq,ltq
    ibes_a = pd.read_csv(useful_dir + 'ibes_ann_est.csv', low_memory=False)
    ibes_a = ibes_a.drop(['fyearq', 'fqtr'], axis=1)
    ibes_a = ibes_a.dropna(subset='curcdq')
    ibes_a = ibes_a[ibes_a['gvkey'].isin(gvkeys)]
    ibes_a = ibes_a.rename(columns={'curcdq': 'curr'})
    ibes_a = ibes_a.merge(exch_rates, left_on=['datadate', 'curr'], right_on=['datadate', 'curr']).drop('curr',
                                                                                                                axis=1)

    # not gvkey, datadate; not GBPXXX
    to_export = ibes_a.columns[2:-2]
    for acol in to_export:
        new_acol = acol + '(GBP)'
        ibes_a.loc[:, new_acol] = ibes_a.loc[:, acol] / ibes_a.loc[:, 'GBPXXX']
        ibes_a = ibes_a.drop(acol, axis=1)
        ibes_a.loc[:, new_acol] = ibes_a.loc[:, new_acol].round(4)

        new_df = ibes_a.loc[:, ['gvkey', 'datadate', new_acol]]
        new_df = pivot(new_df)
        new_df.to_csv(by_data_dir + 'fundq/' + acol + '.csv')
        print(acol+' saved!')

    ibes_a.to_csv(raw_output_dir + 'ibes_ann_est.csv', index=False)
    print('ibes_ann_est saved!')

organize_ibesq = True
if organize_ibesq:
    # gvkey,fyearq,fqtr,datadate,curcdq,
    # ibq,saleq,txtq,revtq,cogsq,xsgaq,atq,actq,cheq,lctq,dlcq,ppentq,ceqq,seqq,ltq
    ibes_q = pd.read_csv(useful_dir + 'ibes_qtr_est.csv', low_memory=False)
    ibes_q = ibes_q.drop(['fyearq', 'fqtr'], axis=1)
    ibes_q = ibes_q.dropna(subset='curcdq')
    ibes_q = ibes_q[ibes_q['gvkey'].isin(gvkeys)]
    ibes_q = ibes_q.rename(columns={'curcdq': 'curr'})
    ibes_q = ibes_q.merge(exch_rates, left_on=['datadate', 'curr'], right_on=['datadate', 'curr']).drop('curr',
                                                                                                                axis=1)

    # not gvkey, datadate; not GBPXXX
    to_export = ibes_q.columns[2:-2]
    for acol in to_export:
        new_acol = acol + '(GBP)'
        ibes_q.loc[:, new_acol] = ibes_q.loc[:, acol] / ibes_q.loc[:, 'GBPXXX']
        ibes_q = ibes_q.drop(acol, axis=1)
        ibes_q.loc[:, new_acol] = ibes_q.loc[:, new_acol].round(4)

        new_df = ibes_q.loc[:, ['gvkey', 'datadate', new_acol]]
        new_df = pivot(new_df)
        new_df.to_csv(by_data_dir + 'fundq/' + acol + '.csv')
        print(acol+' saved!')

    ibes_q.to_csv(raw_output_dir + 'ibes_qtr_est.csv', index=False)
    print('ibes_qtr_est saved!')

