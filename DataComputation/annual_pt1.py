import os
import pandas as pd
from numpy import log
import logging
from elementwise_calc import lag, delta, rate_of_change, ind_adj

date_col = pd.read_csv('../files/metadata/business_days.csv')[['YYYY-MM-DD', 'YYYYMMDD']]

funda_dir = '../files/by_data/funda/'
ref_dir = '../files/by_data/from_ref/'
secd_dir = '../files/by_data/secd_ref/'
seventyeight_dir = './files/by_data/seventyeight/'

is_first = pd.read_csv(ref_dir + 'count.csv') == 1

if __name__ == '__main__':
    data_df = pd.DataFrame()
    for file_name in os.listdir(ref_dir):
        to_concat = pd.read_csv(ref_dir + file_name)
        to_concat['data_name'] = file_name[:-4]
        data_df = pd.concat([data_df, to_concat], axis=0)
    data_df = data_df.set_index('datadate')

    if True:
        logging.warning('Running annual_pt1.py will consume huge amount of memory!')

        aco = data_df[data_df['data_name'] == 'aco'].drop('data_name', axis=1).fillna(0)
        act = data_df[data_df['data_name'] == 'act'].drop('data_name', axis=1).fillna(0)
        ao = data_df[data_df['data_name'] == 'ao'].drop('data_name', axis=1).fillna(0)
        ap = data_df[data_df['data_name'] == 'ap'].drop('data_name', axis=1).fillna(0)
        at = data_df[data_df['data_name'] == 'at'].drop('data_name', axis=1).fillna(0)
        capx = data_df[data_df['data_name'] == 'capx'].drop('data_name', axis=1).fillna(0)
        ceq = data_df[data_df['data_name'] == 'ceq'].drop('data_name', axis=1).fillna(0)
        che = data_df[data_df['data_name'] == 'che'].drop('data_name', axis=1).fillna(0)
        cogs = data_df[data_df['data_name'] == 'cogs'].drop('data_name', axis=1).fillna(0)
        count = data_df[data_df['data_name'] == 'count'].drop('data_name', axis=1).fillna(0)
        credrat = data_df[data_df['data_name'] == 'credrat'].drop('data_name', axis=1).fillna(0)
        dc = data_df[data_df['data_name'] == 'dc'].drop('data_name', axis=1).fillna(0)
        dcpstk = data_df[data_df['data_name'] == 'dcpstk'].drop('data_name', axis=1).fillna(0)
        dcvt = data_df[data_df['data_name'] == 'dcvt'].drop('data_name', axis=1).fillna(0)
        dlc = data_df[data_df['data_name'] == 'dlc'].drop('data_name', axis=1).fillna(0)
        dltt = data_df[data_df['data_name'] == 'dltt'].drop('data_name', axis=1).fillna(0)
        dm = data_df[data_df['data_name'] == 'dm'].drop('data_name', axis=1).fillna(0)
        dp = data_df[data_df['data_name'] == 'dp'].drop('data_name', axis=1).fillna(0)
        dr = data_df[data_df['data_name'] == 'dr'].drop('data_name', axis=1).fillna(0)
        drc = data_df[data_df['data_name'] == 'drc'].drop('data_name', axis=1).fillna(0)
        drlt = data_df[data_df['data_name'] == 'drlt'].drop('data_name', axis=1).fillna(0)
        dvt = data_df[data_df['data_name'] == 'dvt'].drop('data_name', axis=1).fillna(0)
        ebit = data_df[data_df['data_name'] == 'ebit'].drop('data_name', axis=1).fillna(0)
        ebitda = data_df[data_df['data_name'] == 'ebitda'].drop('data_name', axis=1).fillna(0)
        emp = data_df[data_df['data_name'] == 'emp'].drop('data_name', axis=1).fillna(0)
        fatl = data_df[data_df['data_name'] == 'fatl'].drop('data_name', axis=1).fillna(0)
        fatb = data_df[data_df['data_name'] == 'fatb'].drop('data_name', axis=1).fillna(0)
        gdwlia = data_df[data_df['data_name'] == 'gdwlia'].drop('data_name', axis=1).fillna(0)
        ib = data_df[data_df['data_name'] == 'ib'].drop('data_name', axis=1).fillna(0)
        intan = data_df[data_df['data_name'] == 'intan'].drop('data_name', axis=1).fillna(0)
        invt = data_df[data_df['data_name'] == 'invt'].drop('data_name', axis=1).fillna(0)
        lco = data_df[data_df['data_name'] == 'lco'].drop('data_name', axis=1).fillna(0)
        lct = data_df[data_df['data_name'] == 'lct'].drop('data_name', axis=1).fillna(0)
        lo = data_df[data_df['data_name'] == 'lo'].drop('data_name', axis=1).fillna(0)
        lt = data_df[data_df['data_name'] == 'lt'].drop('data_name', axis=1).fillna(0)
        ni = data_df[data_df['data_name'] == 'ni'].drop('data_name', axis=1).fillna(0)
        nopi = data_df[data_df['data_name'] == 'nopi'].drop('data_name', axis=1).fillna(0)
        oancf = data_df[data_df['data_name'] == 'oancf'].drop('data_name', axis=1).fillna(0)
        ob = data_df[data_df['data_name'] == 'ob'].drop('data_name', axis=1).fillna(0)
        pi = data_df[data_df['data_name'] == 'pi'].drop('data_name', axis=1).fillna(0)
        ppegt = data_df[data_df['data_name'] == 'ppegt'].drop('data_name', axis=1).fillna(0)
        ppent = data_df[data_df['data_name'] == 'ppent'].drop('data_name', axis=1).fillna(0)
        pstk = data_df[data_df['data_name'] == 'pstk'].drop('data_name', axis=1).fillna(0)
        pstkq = data_df[data_df['data_name'] == 'pstkq'].drop('data_name', axis=1).fillna(0)
        rect = data_df[data_df['data_name'] == 'rect'].drop('data_name', axis=1).fillna(0)
        revt = data_df[data_df['data_name'] == 'revt'].drop('data_name', axis=1).fillna(0)
        scstkc = data_df[data_df['data_name'] == 'scstkc'].drop('data_name', axis=1).fillna(0)
        spi = data_df[data_df['data_name'] == 'spi'].drop('data_name', axis=1).fillna(0)
        txdi = data_df[data_df['data_name'] == 'txdi'].drop('data_name', axis=1).fillna(0)
        txfed = data_df[data_df['data_name'] == 'txfed'].drop('data_name', axis=1).fillna(0)
        txfo = data_df[data_df['data_name'] == 'txfo'].drop('data_name', axis=1).fillna(0)
        txp = data_df[data_df['data_name'] == 'txp'].drop('data_name', axis=1).fillna(0)
        txt = data_df[data_df['data_name'] == 'txt'].drop('data_name', axis=1).fillna(0)
        xad = data_df[data_df['data_name'] == 'xad'].drop('data_name', axis=1).fillna(0)
        xint = data_df[data_df['data_name'] == 'xint'].drop('data_name', axis=1).fillna(0)
        xrd = data_df[data_df['data_name'] == 'xrd'].drop('data_name', axis=1).fillna(0)
        xsga = data_df[data_df['data_name'] == 'xsga'].drop('data_name', axis=1).fillna(0)
        mve = pd.read_csv(secd_dir + 'mve.csv').fillna(0)
        close = pd.read_csv(secd_dir + 'close.csv').fillna(0)
        csho = pd.read_csv(secd_dir + 'shrout.csv').fillna(0)
        cpi = 0  # todo

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
    print('ten done')
    agr = rate_of_change(at)
    gma = (revt - cogs) / lag(at)
    chcsho = rate_of_change(csho)
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
    print('ten done')
    cfp = oancfisna * (ib - oancf_alt) / mve + oancfnotna * oancf / mve
    cfp_ia = ind_adj(cfp)  # line 244
    absacc = abs(acc)
    age = count
    chinv = atlagat * delta(invt)
    spii = (spi != 0)
    spi = atlagat * spi
    cf = atlagat * (
            oancfnotna * oancf +
            oancfisna * (ib - oancf_alt))
    hire = rate_of_change(emp) * (emp != 0) * (lag(emp) != 0)
    hire = hire.fillna(0)  # it is a valid flllna
    print('ten done')
    chempia = ind_adj(hire)  # line 242
    sgr = rate_of_change(revt)  # sale == revt
    chpm = (ib / revt) - (lag(ib) / lag(revt))
    chpmia = ind_adj(chpm)  # line 241

    mve_ia = ind_adj(mve)  # line 244
    herf = revt.div(revt.sum(axis=1).squeeze(), axis=0) * revt.div(revt.sum(axis=1).squeeze(), axis=0)  # line 250

    bm.to_csv(seventyeight_dir + 'bm.csv')
    ep.to_csv(seventyeight_dir + 'ep.csv')
    cashpr.to_csv(seventyeight_dir + 'cashpr.csv')
    dy.to_csv(seventyeight_dir + 'dy.csv')
    lev.to_csv(seventyeight_dir + 'lev.csv')
    sp.to_csv(seventyeight_dir + 'sp.csv')
    roic.to_csv(seventyeight_dir + 'roic.csv')
    agr.to_csv(seventyeight_dir + 'agr.csv')
    gma.to_csv(seventyeight_dir + 'gma.csv')
    chcsho.to_csv(seventyeight_dir + 'chcsho.csv')
    lgr.to_csv(seventyeight_dir + 'lgr.csv')
    acc.to_csv(seventyeight_dir + 'acc.csv')
    pctacc.to_csv(seventyeight_dir + 'pctacc.csv')
    cfp.to_csv(seventyeight_dir + 'cfp.csv')
    absacc.to_csv(seventyeight_dir + 'absacc.csv')
    age.to_csv(seventyeight_dir + 'age.csv')
    chinv.to_csv(seventyeight_dir + 'chinv.csv')
    hire.to_csv(seventyeight_dir + 'hire.csv')
    sgr.to_csv(seventyeight_dir + 'sgr.csv')


    atlagat.to_csv('files/by_data/intermed/atlagat.csv')
