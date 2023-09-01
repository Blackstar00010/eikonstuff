from numpy import log
import pandas as pd
from os.path import join as pathjoin
import logging
import time
from Misc.elementwise_calc import lag, delta, rate_of_change, ind_adj
import Misc.useful_stuff as us

wrds = True

funda_dir = '../data/processed_wrds/input_funda/' if wrds else '../data/processed/input_funda/'
secd_dir = '../data/processed_wrds/input_secd/' if wrds else '../data/processed/input_secd/'

by_var_dir = '../data/processed_wrds/output_by_var_dd/' if wrds else '../data/processed/output_by_var_dd/'
intermed_dir = '../data/processed_wrds/intermed/' if wrds else '../data/processed/intermed/'


if __name__ == '__main__':
    if True:
        logging.warning('Running annual_pt2.py will consume a huge amount of memory!')
        time.sleep(0.1)
        if input('Do you wish to continue? [y/n] ') not in ['y', 'ㅛ', 'Y']:
            import sys
            sys.exit()

        oancf_alt = pd.read_csv(pathjoin(intermed_dir, 'oancf_alt.csv')).set_index('datadate').fillna(0)
        atlagat = pd.read_csv(pathjoin(intermed_dir, 'atlagat.csv')).set_index('datadate').fillna(0)

        ib = pd.read_csv(pathjoin(funda_dir, 'ib.csv')).set_index('datadate').fillna(0)
        oancf = pd.read_csv(pathjoin(funda_dir, 'oancf.csv')).set_index('datadate').fillna(0)
        invt = pd.read_csv(pathjoin(funda_dir, 'invt.csv')).set_index('datadate').fillna(0)
        revt = pd.read_csv(pathjoin(funda_dir, 'revt.csv')).set_index('datadate').fillna(0)
        at = pd.read_csv(pathjoin(funda_dir, 'at.csv')).set_index('datadate').fillna(0)
        count = pd.read_csv(pathjoin(funda_dir, 'count.csv')).set_index('datadate').fillna(0)
        rect = pd.read_csv(pathjoin(funda_dir, 'rect.csv')).set_index('datadate').fillna(0)
        cogs = pd.read_csv(pathjoin(funda_dir, 'cogs.csv')).set_index('datadate').fillna(0)
        xsga = pd.read_csv(pathjoin(funda_dir, 'xsga.csv')).set_index('datadate').fillna(0)
        dp = pd.read_csv(pathjoin(funda_dir, 'dp.csv')).set_index('datadate').fillna(0)
        ppent = pd.read_csv(pathjoin(funda_dir, 'ppent.csv')).set_index('datadate').fillna(0)
        try:
            xad = pd.read_csv(pathjoin(funda_dir, 'xad.csv')).set_index('datadate').fillna(0)
        except FileNotFoundError:
            xad = ppent.notna() * 0
        ppegt = pd.read_csv(pathjoin(funda_dir, 'ppegt.csv')).set_index('datadate').fillna(0)
        ceq = pd.read_csv(pathjoin(funda_dir, 'ceq.csv')).set_index('datadate').fillna(0)
        capx = pd.read_csv(pathjoin(funda_dir, 'capx.csv')).set_index('datadate').fillna(0)
        try:
            gdwlia = pd.read_csv(pathjoin(funda_dir, 'gdwlia.csv')).set_index('datadate').fillna(0)
        except FileNotFoundError:
            gdwlia = pd.read_csv(pathjoin(funda_dir, 'gdwl.csv')).set_index('datadate').fillna(0)
        che = pd.read_csv(pathjoin(funda_dir, 'che.csv')).set_index('datadate').fillna(0)
        act = pd.read_csv(pathjoin(funda_dir, 'act.csv')).set_index('datadate').fillna(0)
        lct = pd.read_csv(pathjoin(funda_dir, 'lct.csv')).set_index('datadate').fillna(0)
        ap = pd.read_csv(pathjoin(funda_dir, 'ap.csv')).set_index('datadate').fillna(0)
        lt = pd.read_csv(pathjoin(funda_dir, 'lt.csv')).set_index('datadate').fillna(0)
        fatb = pd.read_csv(pathjoin(funda_dir, 'fatl.csv')).set_index('datadate').fillna(0)
        fatl = pd.read_csv(pathjoin(funda_dir, 'fatl.csv')).set_index('datadate').fillna(0)

        print('all data loaded.')

    oancfisna = oancf.isna() + (oancf == 0)
    oancfnotna = ~oancfisna

    chato = 2 * (revt / (at + lag(at))) - (lag(revt) / (lag(at) + lag(at, by=2)))
    chato = chato * (count >= 3) + 0 * (count < 3)
    chatoia = ind_adj(chato)
    pchsale_pchinvt = rate_of_change(revt) - rate_of_change(invt)
    pchsale_pchrect = rate_of_change(revt) - rate_of_change(rect)
    pchgm_pchsale = rate_of_change(revt - cogs) - rate_of_change(revt)
    pchsale_pchxsga = rate_of_change(revt) - rate_of_change(xsga)
    depr = dp / ppent
    pchdepr = rate_of_change(depr)
    # chadv = log(1 + xad) - log(1 + lag(xad))
    invest = (delta(ppegt.replace(0, float('NaN')).fillna(ppent)) + delta(invt)) / lag(at)
    egr = rate_of_change(ceq)
    print('ten calculated!')
    capx = capx.replace(0, float('NaN')).fillna(delta(ppent))
    pchcapx = rate_of_change(capx)
    pchcapx_ia = ind_adj(pchcapx)
    grcapx = (capx - lag(capx, by=2)) / lag(capx, by=2)
    grcapx = grcapx * (count >= 3) + 0 * (count < 3)
    # grGW = rate_of_change(gdwlia)
    # grGW = (gdwlia == 0) * 0 + ((gdwlia != 0) * (grGW.isna())) * 1 + (grGW.notna()) * grGW  # legit isna/notna
    # woGW = (gdwlia != 0)
    # tang = (che + rect * 0.715 + invt * 0.547 + ppent * 0.535) / at
    # if (2100<=sic<=2199) or (2080<=sic<=2085) or (naics in ('7132','71312','713210','71329','713290','72112','721120'))
    #     then sin=1 else sin=0  # TODO: (174)
    act = act.replace(0, float('NaN')).fillna(che + rect + invt)
    lct = lct.replace(0, float('NaN')).fillna(ap)
    currat = act / lct
    print('ten calculated!')
    pchcurrat = rate_of_change(act / lct)
    quick = (act - invt) / lct
    pchquick = rate_of_change((act - invt) / lct)
    salecash = revt / che
    salerec = revt / rect
    saleinv = revt / invt
    pchsaleinv = rate_of_change(revt / invt)
    cashdebt = 2 * (ib + dp) / (lt + lag(lt))
    # realestate = (fatb + fatl) / (ppegt.replace(0, float('NaN')).fillna(ppent))
    print('ten calculated!')

    chatoia.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(pathjoin(by_var_dir, 'chatoia.csv'))
    pchsale_pchinvt.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(pathjoin(by_var_dir, 'pchsale_pchinvt.csv'))
    pchsale_pchrect.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(pathjoin(by_var_dir, 'pchsale_pchrect.csv'))
    pchgm_pchsale.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(pathjoin(by_var_dir, 'pchgm_pchsale.csv'))
    pchsale_pchxsga.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(pathjoin(by_var_dir, 'pchsale_pchxsga.csv'))
    depr.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(pathjoin(by_var_dir, 'depr.csv'))
    pchdepr.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(pathjoin(by_var_dir, 'pchdepr.csv'))
    # chadv.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(pathjoin(seventyeight_dir, 'chadv.csv'))
    invest.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(pathjoin(by_var_dir, 'invest.csv'))
    egr.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(pathjoin(by_var_dir, 'egr.csv'))

    # capx.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(pathjoin(seventyeight_dir, 'capx.csv'))
    # pchcapx.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(pathjoin(seventyeight_dir, 'pchcapx.csv'))
    pchcapx_ia.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(pathjoin(by_var_dir, 'pchcapx_ia.csv'))
    grcapx.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(pathjoin(by_var_dir, 'grcapx.csv'))
    # grGW.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(pathjoin(seventyeight_dir, 'grGW.csv'))
    # woGW.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(pathjoin(seventyeight_dir, 'woGW.csv'))
    # tang.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(pathjoin(seventyeight_dir, 'tang.csv'))
    currat.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(pathjoin(by_var_dir, 'currat.csv'))

    pchcurrat.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(pathjoin(by_var_dir, 'pchcurrat.csv'))
    quick.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(pathjoin(by_var_dir, 'quick.csv'))
    pchquick.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(pathjoin(by_var_dir, 'pchquick.csv'))
    salecash.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(pathjoin(by_var_dir, 'salecash.csv'))
    salerec.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(pathjoin(by_var_dir, 'salerec.csv'))
    saleinv.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(pathjoin(by_var_dir, 'saleinv.csv'))
    pchsaleinv.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(pathjoin(by_var_dir, 'pchsaleinv.csv'))
    cashdebt.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(pathjoin(by_var_dir, 'cashdebt.csv'))

    us.beep()
