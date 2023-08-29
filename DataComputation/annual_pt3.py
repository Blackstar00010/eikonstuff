import pandas as pd
import logging
from Misc.elementwise_calc import lag, delta, rate_of_change, ind_adj
import Misc.useful_stuff as us
from os.path import join as pathjoin
import time

wrds = False

funda_dir = '../data/processed_wrds/input_funda/' if wrds else '../data/processed/input_funda/'
secd_dir = '../data/processed_wrds/input_secd/' if wrds else '../data/processed/input_secd/'
by_var_dir = '../data/processed_wrds/output_by_var_dd/' if wrds else '../data/processed/output_by_var_dd/'
intermed_dir = '../data/processed_wrds/intermed/' if wrds else '../data/processed/intermed/'

is_first = pd.read_csv(funda_dir + 'count.csv') == 1

if __name__ == '__main__':
    if True:
        logging.warning('Running this file will consume huge amount of memory!')
        time.sleep(0.1)
        if input('Do you wish to continue? [y/n] ') != 'y':
            import sys
            sys.exit()

        atlagat = pd.read_csv(pathjoin(intermed_dir, 'atlagat.csv')).set_index('datadate').fillna(0)
        csho = pd.read_csv(pathjoin(secd_dir, 'shrout.csv'))
        csho = csho.set_index(us.date_col_finder(csho, 'csho')).fillna(0)

        # ob
        dltt = pd.read_csv(pathjoin(funda_dir, 'dltt.csv')).set_index('datadate').fillna(0)
        try:
            dm = pd.read_csv(pathjoin(funda_dir, 'dm.csv')).set_index('datadate').fillna(0)
        except FileNotFoundError:
            dm = dltt
        try:
            dc = pd.read_csv(pathjoin(funda_dir, 'dc.csv')).set_index('datadate').fillna(0)
        except FileNotFoundError:
            dc = dltt
        # dr
        ppent = pd.read_csv(pathjoin(funda_dir, 'ppent.csv')).set_index('datadate').fillna(0)
        intan = pd.read_csv(pathjoin(funda_dir, 'intan.csv')).set_index('datadate').fillna(0)
        ao = pd.read_csv(pathjoin(funda_dir, 'ao.csv')).set_index('datadate').fillna(0)
        lo = pd.read_csv(pathjoin(funda_dir, 'lo.csv')).set_index('datadate').fillna(0)
        dp = pd.read_csv(pathjoin(funda_dir, 'dp.csv')).set_index('datadate').fillna(0)
        xrd = pd.read_csv(pathjoin(funda_dir, 'xrd.csv')).set_index('datadate').fillna(0)
        at = pd.read_csv(pathjoin(funda_dir, 'at.csv')).set_index('datadate').fillna(0)
        ib = pd.read_csv(pathjoin(funda_dir, 'ib.csv')).set_index('datadate').fillna(0)
        ceq = pd.read_csv(pathjoin(funda_dir, 'ceq.csv')).set_index('datadate').fillna(0)
        revt = pd.read_csv(pathjoin(funda_dir, 'revt.csv')).set_index('datadate').fillna(0)
        cogs = pd.read_csv(pathjoin(funda_dir, 'cogs.csv')).set_index('datadate').fillna(0)
        xsga = pd.read_csv(pathjoin(funda_dir, 'xsga.csv')).set_index('datadate').fillna(0)
        xint = pd.read_csv(pathjoin(funda_dir, 'xint.csv')).set_index('datadate').fillna(0)
        oancf = pd.read_csv(pathjoin(funda_dir, 'oancf.csv')).set_index('datadate').fillna(0)
        act = pd.read_csv(pathjoin(funda_dir, 'act.csv')).set_index('datadate').fillna(0)
        lct = pd.read_csv(pathjoin(funda_dir, 'lct.csv')).set_index('datadate').fillna(0)
        txt = pd.read_csv(pathjoin(funda_dir, 'txt.csv')).set_index('datadate').fillna(0)
        try:
            ni = pd.read_csv(pathjoin(funda_dir, 'ni.csv')).set_index('datadate').fillna(0)
        except FileNotFoundError:
            ni = ib - txt
        try:
            scstkc = pd.read_csv(pathjoin(funda_dir, 'scstkc.csv')).set_index('datadate').fillna(0)
        except:
            prccd = pd.read_csv(pathjoin(secd_dir, 'close.csv'))
            prccd = prccd.set_index(us.date_col_finder(prccd, 'prccd'))
            scstkc = prccd * delta(csho)
            scstkc = scstkc.fillna(method='ffill')

        print('all data loaded.')

    # obklg = ob * atlagat
    # chobklg = delta(obklg) * atlagat
    securedind = (dm != 0) * 1
    secured = dm / dltt
    convind = ((dc != 0) + (csho > 0)) * 1
    # conv = dc / dltt
    grltnoa = (delta(+ppent + intan + ao - lo) - dp) * atlagat
    # chdrc = delta(dr) * atlagat
    print('ten done')
    rd = (rate_of_change(xrd / at) > .05) * 1
    # roe = ib / lag(ceq)
    # rdbias = rate_of_change(xrd) - roe
    operprof = (revt - cogs - xsga - xint) / lag(ceq)
    new_oancf = pd.concat([oancf, pd.DataFrame(float('NaN'),
                                               columns=ni.columns[~ni.columns.isin(oancf.columns)],
                                               index=ni.index[~ni.index.isin(oancf.index)])])
    new_ni = pd.concat([ni, pd.DataFrame(float('NaN'),
                                         columns=oancf.columns[~oancf.columns.isin(ni.columns)],
                                         index=oancf.index[~oancf.index.isin(ni.index)])])
    new_oancf = new_oancf.reindex(columns=new_ni.columns, index=new_ni.index)
    ps = (ni > 0) * 1 + (oancf > 0) * 1 + (ni / at > lag(ni / at)) * 1 + (new_oancf > new_ni) * 1 + \
         (dltt / at < lag(dltt / at)) * 1 + (act / lct > lag(act / lct)) * 1 + \
         ((revt - cogs) / revt > (lag((revt - cogs) / revt))) * 1 + (revt / at > lag(revt / at)) * 1 + (scstkc == 0) * 1

    # TODO: Lev and Nissim (2004) https://www.jstor.org/stable/4093085
    tr = 1  # todo: (206)
    tb = txt / tr / ib * (ib > 0) + 1 * (ib <= 0)
    tb = ind_adj(tb)

    securedind.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(pathjoin(by_var_dir, 'securedind.csv'))
    secured.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(pathjoin(by_var_dir, 'secured.csv'))
    convind.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(pathjoin(by_var_dir, 'convind.csv'))
    grltnoa.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(pathjoin(by_var_dir, 'grltnoa.csv'))
    rd.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(pathjoin(by_var_dir, 'rd.csv'))
    operprof.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(pathjoin(by_var_dir, 'operprof.csv'))
    ps.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(pathjoin(by_var_dir, 'ps.csv'))
    tb.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(pathjoin(by_var_dir, 'tb.csv'))

    us.beep()
