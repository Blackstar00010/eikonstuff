from os.path import join as pathjoin
import shutil
import pandas as pd
import logging
import time
from Misc.elementwise_calc import lag, delta, rate_of_change, ind_adj
import Misc.useful_stuff as us
import _options as opt
import sys

wrds = opt.wrds

funda_dir = opt.funda_dir
fundq_dir = opt.fundq_dir
secd_dir = opt.secd_dir
intermed_dir = opt.intermed_dir
by_var_dir = opt.by_var_dd_dir


def run_annual1(beep_on_finish=True):
    if __name__ != '__main__':
        print('Computing the first batch of annually updated firm characteristics...')
    if True:
        act = pd.read_csv(funda_dir + 'act.csv').set_index('datadate').fillna(0)
        at = pd.read_csv(funda_dir + 'at.csv').set_index('datadate').fillna(0)
        ceq = pd.read_csv(funda_dir + 'ceq.csv').set_index('datadate').fillna(0)
        che = pd.read_csv(funda_dir + 'che.csv').set_index('datadate').fillna(0)
        cogs = pd.read_csv(funda_dir + 'cogs.csv').set_index('datadate').fillna(0)
        dlc = pd.read_csv(funda_dir + 'dlc.csv').set_index('datadate').fillna(0)
        dltt = pd.read_csv(funda_dir + 'dltt.csv').set_index('datadate').fillna(0)
        dp = pd.read_csv(funda_dir + 'dp.csv').set_index('datadate').fillna(0)
        dvt = pd.read_csv(funda_dir + 'dvt.csv').set_index('datadate').fillna(0)
        ebit = pd.read_csv(funda_dir + 'ebit.csv').set_index('datadate').fillna(0)
        print('ten imported!')
        emp = pd.read_csv(funda_dir + 'emp.csv').set_index('datadate').fillna(0)
        ib = pd.read_csv(funda_dir + 'ib.csv').set_index('datadate').fillna(0)
        invt = pd.read_csv(funda_dir + 'invt.csv').set_index('datadate').fillna(0)
        lct = pd.read_csv(funda_dir + 'lct.csv').set_index('datadate').fillna(0)
        lt = pd.read_csv(funda_dir + 'lt.csv').set_index('datadate').fillna(0)
        nopi = pd.read_csv(funda_dir + 'nopi.csv').set_index('datadate').fillna(0)
        oancf = pd.read_csv(funda_dir + 'oancf.csv').set_index('datadate').fillna(0)
        revt = pd.read_csv(funda_dir + 'revt.csv').set_index('datadate').fillna(0)
        txp = pd.read_csv(funda_dir + 'txp.csv').set_index('datadate').fillna(0)
        xrd = pd.read_csv(funda_dir + 'xrd.csv').set_index('datadate').fillna(0)
        print('ten imported!')
        mve = pd.read_csv(secd_dir + 'mve.csv').rename(columns={'Date': 'datadate'}).set_index('datadate').fillna(0)
        csho = pd.read_csv(secd_dir + 'shrout.csv').rename(columns={'Date': 'datadate'}).set_index('datadate').fillna(0)

        print('all data loaded.')

    # line 122 - 134
    bm = ceq / mve
    bm_ia = ind_adj(bm)  # line 242
    ep = ib / mve
    cashpr = ((mve + dltt - at) / che)
    dy = dvt / mve
    lev = lt / mve
    sp = revt / mve
    roic = (ebit - nopi) / (ceq + lt - che)
    rd_sale = xrd / revt
    rd_mve = xrd / mve
    print('ten calculated!')
    agr = rate_of_change(at)
    gma = (revt - cogs) / lag(at)
    chcsho = rate_of_change(csho).replace(-1.0, float('NaN'))
    lgr = rate_of_change(lt)

    oancfisna = oancf.isna() + (oancf == 0)
    oancfnotna = ~oancfisna
    atlagat = 2 / (at + lag(at))
    oancf_alt = delta(act) - delta(che) - delta(lct) + delta(dlc) + delta(txp) + dp

    # line 135-153
    acc = atlagat * (
            oancfnotna * (ib - oancf) +
            oancfisna * oancf_alt)
    pctacc = (oancfnotna * (ib - oancf) / (abs(ib) * (ib != 0) + 0.1 * (ib == 0)) +
              oancfisna * oancf_alt / (abs(ib) * (ib != 0) + 0.1 * (ib == 0)))
    print('ten calculated!')
    absacc = abs(acc)
    cfp = (oancfisna * (ib - oancf_alt) + oancfnotna * oancf) / mve.replace(0, float('NaN'))
    cfp = cfp.replace(float('NaN'), 0)
    cfp_ia = ind_adj(cfp)  # line 244
    shutil.copyfile(funda_dir + 'count.csv', by_var_dir + 'age.csv')
    chinv = atlagat * delta(invt)
    # spii = (spi != 0)
    # spi = atlagat * spi
    # cf = atlagat * (
    #         oancfnotna * oancf +
    #         oancfisna * (ib - oancf_alt))
    hire = rate_of_change(emp) * (emp != 0) * (lag(emp) != 0)
    hire = hire.fillna(0)  # it is a valid flllna
    chempia = ind_adj(hire)  # line 242
    sgr = rate_of_change(revt)  # sale == revt
    chpm = (ib / revt) - (lag(ib) / lag(revt))
    print('ten calculated!')
    chpmia = ind_adj(chpm)  # line 241

    mve_ia = ind_adj(mve)  # line 244
    herf = revt.div(revt.sum(axis=1).squeeze(), axis=0) * revt.div(revt.sum(axis=1).squeeze(), axis=0)  # line 250
    divi = ((dvt > 0) * ((lag(dvt) == 0) + lag(dvt).isna())) * 1  # starts paying dividends, legit isna
    divo = (((dvt == 0) + dvt.isna()) * (lag(dvt) > 0)) * 1  # stops paying dividends, legit isna

    print('Saving...')
    bm.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(pathjoin(by_var_dir, 'bm.csv'))
    bm_ia.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(pathjoin(by_var_dir, 'bm_ia.csv'))
    ep.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(pathjoin(by_var_dir, 'ep.csv'))
    cashpr.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(pathjoin(by_var_dir, 'cashpr.csv'))
    dy.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(pathjoin(by_var_dir, 'dy.csv'))
    lev.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(pathjoin(by_var_dir, 'lev.csv'))
    sp.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(pathjoin(by_var_dir, 'sp.csv'))
    roic.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(pathjoin(by_var_dir, 'roic.csv'))
    rd_sale.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(pathjoin(by_var_dir, 'rd_sale.csv'))
    rd_mve.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(pathjoin(by_var_dir, 'rd_mve.csv'))
    print('ten calculated!')
    agr.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(pathjoin(by_var_dir, 'agr.csv'))
    gma.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(pathjoin(by_var_dir, 'gma.csv'))
    chcsho.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(pathjoin(by_var_dir, 'chcsho.csv'))
    lgr.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(pathjoin(by_var_dir, 'lgr.csv'))
    acc.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(pathjoin(by_var_dir, 'acc.csv'))
    pctacc.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(pathjoin(by_var_dir, 'pctacc.csv'))
    cfp.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(pathjoin(by_var_dir, 'cfp.csv'))
    cfp_ia.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(pathjoin(by_var_dir, 'cfp_ia.csv'))
    absacc.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(pathjoin(by_var_dir, 'absacc.csv'))
    # age.replace(0, float('NaN').replace()float('inf'), float('NaN'.to_csv(pathjoin(seventyeight_dir, 'age.csv'))
    chinv.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(pathjoin(by_var_dir, 'chinv.csv'))
    print('ten calculated!')
    hire.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(pathjoin(by_var_dir, 'hire.csv'))
    chempia.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(pathjoin(by_var_dir, 'chempia.csv'))
    sgr.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(pathjoin(by_var_dir, 'sgr.csv'))
    chpmia.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(pathjoin(by_var_dir, 'chpmia.csv'))
    mve_ia.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(pathjoin(by_var_dir, 'mve_ia.csv'))
    herf.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(pathjoin(by_var_dir, 'herf.csv'))
    divi.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(pathjoin(by_var_dir, 'divi.csv'))
    divo.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(pathjoin(by_var_dir, 'divo.csv'))

    atlagat.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(pathjoin(intermed_dir, 'atlagat.csv'))
    oancf_alt.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(pathjoin(intermed_dir, 'oancf_alt.csv'))
    print('ten calculated!')

    if beep_on_finish:
        us.beep()


if __name__ == '__main__':
    logging.warning('Running annual_pt1.py will consume a huge amount of memory!')
    time.sleep(0.1)
    if input('Do you wish to continue? [y/n] ') not in ['y', 'ㅛ', 'Y']:
        sys.exit()

    run_annual1()
