import os
import pandas as pd
from numpy import log
import logging
from Misc.elementwise_calc import lag, delta, rate_of_change, ind_adj

date_col = pd.read_csv('../data/metadata/business_days.csv')[['YYYY-MM-DD', 'YYYYMMDD']]

funda_dir = '../data/processed/funda/'
ref_dir = '../data/processed/input_funda/'
secd_dir = '../data/processed/input_secd/'
seventyeight_dir = './data/processed/output_by_var_dd/'

is_first = pd.read_csv(ref_dir + 'count.csv') == 1

if __name__ == '__main__':
    data_df = pd.DataFrame()
    for file_name in os.listdir(ref_dir):
        to_concat = pd.read_csv(ref_dir + file_name)
        to_concat['data_name'] = file_name[:-4]
        data_df = pd.concat([data_df, to_concat], axis=0)
    data_df = data_df.set_index('datadate')

    if True:
        logging.warning('Running this file will consume huge amount of memory!')
        if input('Do you wish to continue? [y/n] ') != 'y':
            import sys
            sys.exit()

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

    bm = ceq / mve  # pt1
    ep = ib / mve  # pt1
    cashpr = ((mve + dltt - at) / che)  # pt1
    dy = dvt / mve  # pt1
    lev = lt / mve  # pt1
    sp = revt / mve  # pt1
    roic = (ebit - nopi) / (ceq + lt - che)  # pt1
    rd_sale = xrd / revt  # pt1
    rd_mve = xrd / mve  # pt1
    agr = rate_of_change(at)  # pt1
    print('ten done')
    gma = (revt - cogs) / lag(at)  # pt1
    chcsho = rate_of_change(csho)  # pt1
    lgr = rate_of_change(lt)  # pt1

    oancfisna = oancf.isna() + (oancf == 0)  # pt1
    oancfnotna = ~oancfisna  # pt1
    atlagat = 2 / (at + lag(at))  # pt1

    oancf_alt = delta(act) - delta(che) - delta(lct) + delta(dlc) + delta(txp) + dp  # pt1
    acc = atlagat * (
            oancfnotna * (ib - oancf) +
            oancfisna * oancf_alt)  # pt1
    pctacc = (oancfnotna * (ib - oancf) / (abs(ib) * (ib != 0) + 0.1 * (ib == 0)) +
              oancfisna * oancf_alt / (abs(ib) * (ib != 0) + 0.1 * (ib == 0)))  # pt1
    cfp = oancfisna * (ib - oancf_alt) / mve + oancfnotna * oancf / mve  # pt1
    print('ten done')
    absacc = abs(acc)  # pt1
    age = count  # pt1
    chinv = atlagat * delta(invt)  # pt1
    spii = (spi != 0)  # pt1
    spi = atlagat * spi  # pt1
    cf = atlagat * (
            oancfnotna * oancf +
            oancfisna * (ib - oancf_alt))  # pt1
    hire = rate_of_change(emp) * (emp != 0) * (lag(emp) != 0)  # pt1
    hire = hire.fillna(0)  # it is a valid flllna | pt1
    sgr = rate_of_change(revt)  # sale == revt | pt1
    chpm = (ib / revt) - (lag(ib) / lag(revt))  # pt1
    print('ten done')
    chato = 2 * (revt / (at + lag(at))) - (lag(revt) / (lag(at) + lag(at, by=2)))  # pt2
    pchsale_pchinvt = rate_of_change(revt) - rate_of_change(invt)  # pt2
    pchsale_pchrect = rate_of_change(revt) - rate_of_change(rect)  # pt2
    pchgm_pchsale = rate_of_change(revt - cogs) - rate_of_change(revt)  # pt2
    pchsale_pchxsga = rate_of_change(revt) - rate_of_change(xsga)  # pt2
    depr = dp / ppent  # pt2
    pchdepr = rate_of_change(depr)  # pt2
    chadv = log(1 + xad) - log(1 + lag(xad))  # pt2
    invest = (delta(ppegt.replace(0, float('NaN')).fillna(ppent)) + delta(invt)) / lag(at)  # pt2
    egr = rate_of_change(ceq)  # pt2
    print('ten done')
    capx = capx.replace(0, float('NaN')).fillna(delta(ppent))  # pt2
    pchcapx = rate_of_change(capx)  # pt2
    grcapx = (capx - lag(capx, by=2)) / lag(capx, by=2)  # pt2
    grGW = rate_of_change(gdwlia)  # pt2
    grGW = (gdwlia == 0) * 0 + ((gdwlia != 0) * (grGW.isna())) * 1 + (grGW.notna()) * grGW  # legit isna/notna | pt2
    woGW = (gdwlia != 0)  # pt2
    tang = (che + rect * 0.715 + invt * 0.547 + ppent * 0.535) / at  # pt2
    # if (2100<=sic<=2199) or (2080<=sic<=2085) or (naics in ('7132','71312','713210','71329','713290','72112','721120'))
    #     then sin=1 else sin=0  # TODO: (174)
    act = act.replace(0, float('NaN')).fillna(che + rect + invt)  # pt2
    lct = lct.replace(0, float('NaN')).fillna(ap)  # pt2
    currat = act / lct  # pt2
    print('ten done')
    pchcurrat = rate_of_change(act / lct)  # pt2
    quick = (act - invt) / lct  # pt2
    pchquick = rate_of_change((act - invt) / lct)  # pt2
    salecash = revt / che  # pt2
    salerec = revt / rect  # pt2
    saleinv = revt / invt  # pt2
    pchsaleinv = rate_of_change(revt / invt)  # pt2
    cashdebt = 2 * (ib + dp) / (lt + lag(lt))  # pt2
    realestate = (fatb + fatl) / (ppegt.replace(0, float('NaN')).fillna(ppent))  # pt2
    print('ten done')
    divi = ((dvt > 0) * ((lag(dvt) == 0) + lag(dvt).isna())) * 1  # starts paying dividends, legit isna | pt1
    divo = (((dvt == 0) + dvt.isna()) * (lag(dvt) > 0)) * 1  # stops paying dividends, legit isna | pt1
    obklg = ob * atlagat  # pt3
    chobklg = delta(ob) * atlagat  # pt3
    securedind = (dm != 0) * 1  # pt3
    secured = dm / dltt  # pt3
    convind = ((dc != 0) + (csho > 0)) * 1  # pt3
    conv = dc / dltt  # pt3
    grltnoa = (delta(+ppent + intan + ao - lo) - dp) / (at + lag(at)) * 2  # pt3
    chdrc = delta(dr) / (at + lag(at)) * 2  # pt3
    print('ten done')
    rd = (rate_of_change(xrd / at) > .05) * 1  # pt3
    roe = ib / lag(ceq)  # pt3
    rdbias = rate_of_change(xrd) - roe  # pt3
    operprof = (revt - cogs - xsga - xint) / lag(ceq)  # pt3
    ps = (ni > 0) * 1 + (oancf > 0) * 1 + (ni / at > lag(ni / at)) * 1 + (oancf > ni) * 1 + (
            dltt / at < lag(dltt / at)) * 1 + (act / lct > lag(act / lct)) * 1 + (
                 (revt - cogs) / revt > (lag((revt - cogs) / revt)) * 1) + (revt / at > lag(revt / at)) * 1 + (
                 scstkc == 0) * 1  # pt3

    # TODO: Lev and Nissim (2004) https://www.jstor.org/stable/4093085
    tr = 0  # todo: (206) | pt3
    tb = txt / tr / ib * (ib > 0) + 1 * (ib <= 0)  # pt3

    chato = chato * (count >= 3) + 0 * (count < 3)  # pt2
    grcapx = grcapx * (count >= 3) + 0 * (count < 3)  # pt2

    chpmia = ind_adj(chpm)  # pt1
    chatoia = ind_adj(chato)  # pt2
    chempia = ind_adj(hire)  # pt1
    bm_ia = ind_adj(bm)  # pt1
    pchcapx_ia = ind_adj(pchcapx)  # pt2
    tb = ind_adj(tb)  # pt3 todo: not tb_ia?
    cfp_ia = ind_adj(cfp)  # pt1
    mve_ia = ind_adj(mve)  # pt1
    herf = revt.div(revt.sum(axis=1).squeeze(), axis=0) * revt.div(revt.sum(axis=1).squeeze(), axis=0)  # pt1

    credrat_dwn = (credrat < lag(credrat)) * 1 * (count > 1)  # pt0

    first_index = xsga.first_valid_index  # pt0
    orgcap = (xsga / cpi / 0.25) * (is_first + (xsga.index == xsga.first_valid_index()))  # pt0
    for i in range(len(orgcap)):
        # todo (385-) | pt0
        continue
    orgcap = atlagat * orgcap * (count > 1)  # pt0

    # todo: mve (516) | pt0
    # todo: pps (518) | pt0

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
    pchsale_pchrect.to_csv(seventyeight_dir + 'pchsale_pchrect.csv')
    pchgm_pchsale.to_csv(seventyeight_dir + 'pchgm_pchsale.csv')
    depr.to_csv(seventyeight_dir + 'depr.csv')
    pchdepr.to_csv(seventyeight_dir + 'pchdepr.csv')
    invest.to_csv(seventyeight_dir + 'invest.csv')
    egr.to_csv(seventyeight_dir + 'egr.csv')
    tang.to_csv(seventyeight_dir + 'tang.csv')
    # sin.to_csv(seventyeight_dir + 'sin.csv')
    currat.to_csv(seventyeight_dir + 'currat.csv')
    pchcurrat.to_csv(seventyeight_dir + 'pchcurrat.csv')
    salecash.to_csv(seventyeight_dir + 'salecash.csv')
    salerec.to_csv(seventyeight_dir + 'salerec.csv')
    cashdebt.to_csv(seventyeight_dir + 'cashdebt.csv')
    divi.to_csv(seventyeight_dir + 'divi.csv')
    divo.to_csv(seventyeight_dir + 'divo.csv')
    securedind.to_csv(seventyeight_dir + 'securedind.csv')
    conv.to_csv(seventyeight_dir + 'conv.csv')
    rd.to_csv(seventyeight_dir + 'rd.csv')
    operprof.to_csv(seventyeight_dir + 'operprof.csv')
    ps.to_csv(seventyeight_dir + 'ps.csv')

    atlagat.to_csv('data/processed/intermed/atlagat.csv')
