import os
import pandas as pd
from numpy import log

date_col = pd.read_csv('files/metadata/business_days.csv')[['YYYY-MM-DD', 'YYYYMMDD']]


def date_column_finder(dataframe: pd.DataFrame):
    """
    Finds the name of the column that contains date values.
    :param dataframe: the dataframe to find a date column from
    :return: str name of the column's name
    """
    date_col_name = [i for i in ['datadate', 'date', 'Datadate', 'DataDate', 'Date'] if i in dataframe.columns]
    if len(date_col_name) < 1:
        date_col_name = dataframe.columns
    if len(date_col_name) > 1:
        goodenough = False
        i = 0
        while goodenough:
            goodenoughQ = input(f'Do you want to use {date_col_name[i]} as the date column? [y/n]')
            if goodenoughQ == 'y':
                goodenough = True
                date_col_name = date_col_name[i]
    else:
        date_col_name = date_col_name[0]
    return date_col_name


def lag(dataframe: pd.DataFrame, by=1):
    """

    :param dataframe:
    :return: a dataframe of calculated delta with the original date column
    """
    ischanged = (dataframe == dataframe.shift(periods=1))
    ret = dataframe.shift(periods=1) * ischanged
    ret = ret.fillna(method='ffill')
    if by > 1:
        ret = lag(ret)
    return ret


def delta(dataframe: pd.DataFrame):
    """
    Calculate absolute changes from the last different values vertically.
    :param dataframe: the dataframe to calculate delta. The column of dates must be the index column.
    :return: a dataframe of calculated delta with the original date column
    """
    ret = dataframe - dataframe.shift(periods=1)
    ret = ret.replace({0: float('NaN')})
    ret = ret.fillna(method='ffill')
    return ret


def rate_of_change(dataframe: pd.DataFrame, minusone=True):
    """
    Calculate absolute changes from the last different values vertically. The value in the returned dataframe is 0.1 if
    the input dataframe's value changed from 10 to 11.
    :param dataframe: the dataframe to calculate delta. Must contain a column of dates.
    :param minusone: For a value changing from 10 to 11, the returned value is 0.1 if minusone=True and 1.1 if minusone=False. True by default.
    :return: a dataframe of calculated rate of change with the original date column
    """
    ret = delta(dataframe)
    ret = ret / (dataframe - ret)
    if not minusone:
        ret += 1
    return ret


funda_dir = 'files/by_data/funda/'
ref_dir = 'files/by_data/from_ref/'
secd_dir = 'files/by_data/secd/'

data_df = pd.DataFrame()
for file_name in os.listdir(ref_dir):
    to_concat = pd.read_csv(ref_dir + file_name)
    to_concat['data_name'] = file_name[:-4]
    data_df = pd.concat([data_df, to_concat], axis=0)
data_df = data_df.set_index('datadate')

if True:
    aco = data_df[data_df['data_name'] == 'aco'].drop('data_name', axis=1)
    act = data_df[data_df['data_name'] == 'act'].drop('data_name', axis=1)
    ao = data_df[data_df['data_name'] == 'ao'].drop('data_name', axis=1)
    ap = data_df[data_df['data_name'] == 'ap'].drop('data_name', axis=1)
    at = data_df[data_df['data_name'] == 'at'].drop('data_name', axis=1)
    capx = data_df[data_df['data_name'] == 'capx'].drop('data_name', axis=1)
    ceq = data_df[data_df['data_name'] == 'ceq'].drop('data_name', axis=1)
    che = data_df[data_df['data_name'] == 'che'].drop('data_name', axis=1)
    cogs = data_df[data_df['data_name'] == 'cogs'].drop('data_name', axis=1)
    count = data_df[data_df['data_name'] == 'count'].drop('data_name', axis=1)
    dc = data_df[data_df['data_name'] == 'dc'].drop('data_name', axis=1)
    dcpstk = data_df[data_df['data_name'] == 'dcpstk'].drop('data_name', axis=1)
    dcvt = data_df[data_df['data_name'] == 'dcvt'].drop('data_name', axis=1)
    dlc = data_df[data_df['data_name'] == 'dlc'].drop('data_name', axis=1)
    dltt = data_df[data_df['data_name'] == 'dltt'].drop('data_name', axis=1)
    dm = data_df[data_df['data_name'] == 'dm'].drop('data_name', axis=1)
    dp = data_df[data_df['data_name'] == 'dp'].drop('data_name', axis=1)
    dr = data_df[data_df['data_name'] == 'dr'].drop('data_name', axis=1)
    drc = data_df[data_df['data_name'] == 'drc'].drop('data_name', axis=1)
    drlt = data_df[data_df['data_name'] == 'drlt'].drop('data_name', axis=1)
    dvt = data_df[data_df['data_name'] == 'dvt'].drop('data_name', axis=1)
    ebit = data_df[data_df['data_name'] == 'ebit'].drop('data_name', axis=1)
    ebitda = data_df[data_df['data_name'] == 'ebitda'].drop('data_name', axis=1)
    emp = data_df[data_df['data_name'] == 'emp'].drop('data_name', axis=1)
    fatl = data_df[data_df['data_name'] == 'fatl'].drop('data_name', axis=1)
    gdwlia = data_df[data_df['data_name'] == 'gdwlia'].drop('data_name', axis=1)
    ib = data_df[data_df['data_name'] == 'ib'].drop('data_name', axis=1)
    intan = data_df[data_df['data_name'] == 'intan'].drop('data_name', axis=1)
    invt = data_df[data_df['data_name'] == 'invt'].drop('data_name', axis=1)
    lco = data_df[data_df['data_name'] == 'lco'].drop('data_name', axis=1)
    lct = data_df[data_df['data_name'] == 'lct'].drop('data_name', axis=1)
    lo = data_df[data_df['data_name'] == 'lo'].drop('data_name', axis=1)
    lt = data_df[data_df['data_name'] == 'lt'].drop('data_name', axis=1)
    ni = data_df[data_df['data_name'] == 'ni'].drop('data_name', axis=1)
    nopi = data_df[data_df['data_name'] == 'nopi'].drop('data_name', axis=1)
    oancf = data_df[data_df['data_name'] == 'oancf'].drop('data_name', axis=1)
    ob = data_df[data_df['data_name'] == 'ob'].drop('data_name', axis=1)
    pi = data_df[data_df['data_name'] == 'pi'].drop('data_name', axis=1)
    ppegt = data_df[data_df['data_name'] == 'ppegt'].drop('data_name', axis=1)
    ppent = data_df[data_df['data_name'] == 'ppent'].drop('data_name', axis=1)
    pstk = data_df[data_df['data_name'] == 'pstk'].drop('data_name', axis=1)
    pstkq = data_df[data_df['data_name'] == 'pstkq'].drop('data_name', axis=1)
    rect = data_df[data_df['data_name'] == 'rect'].drop('data_name', axis=1)
    revt = data_df[data_df['data_name'] == 'revt'].drop('data_name', axis=1)
    scstkc = data_df[data_df['data_name'] == 'scstkc'].drop('data_name', axis=1)
    spi = data_df[data_df['data_name'] == 'spi'].drop('data_name', axis=1)
    txdi = data_df[data_df['data_name'] == 'txdi'].drop('data_name', axis=1)
    txfed = data_df[data_df['data_name'] == 'txfed'].drop('data_name', axis=1)
    txfo = data_df[data_df['data_name'] == 'txfo'].drop('data_name', axis=1)
    txp = data_df[data_df['data_name'] == 'txp'].drop('data_name', axis=1)
    txt = data_df[data_df['data_name'] == 'txt'].drop('data_name', axis=1)
    xad = data_df[data_df['data_name'] == 'xad'].drop('data_name', axis=1)
    xint = data_df[data_df['data_name'] == 'xint'].drop('data_name', axis=1)
    xrd = data_df[data_df['data_name'] == 'xrd'].drop('data_name', axis=1)
    xsga = data_df[data_df['data_name'] == 'xsga'].drop('data_name', axis=1)
    mve = pd.read_csv(secd_dir + 'mve.csv')
    close = pd.read_csv(secd_dir + 'close.csv')
    csho = pd.read_csv(secd_dir + 'shrout.csv')

print('all data loaded.')

bm = ceq / mve
ep = ib / mve
cashpr = ((mve + dltt - at) / che)
dy = dvt / mve
lev = lt / mve
sp = revt / mve
roic = (ebit - nopi) / (ceq + lt - che)
rd_sale = xrd / revt
rd_mve = xrd / mve
agr = rate_of_change(at)
print('ten done')
gma = (revt - cogs) / lag(at)
chcsho = rate_of_change(csho)
lgr = rate_of_change(lt)

oancfisna = oancf.isna() + (oancf == 0)
oancfnotna = ~oancfisna
atlagat = 2 / (at + lag(at))

oancf_alt = delta(act) - delta(che) - delta(lct) + delta(dlc) + delta(txp) + dp
acc = atlagat * (
        oancfnotna * (ib - oancf) +
        oancfisna * oancf_alt)
pctacc = (oancfnotna * (ib - oancf) / (abs(ib) * (ib != 0) + 0.1 * (ib == 0)) +
          oancfisna * oancf_alt / (abs(ib) * (ib != 0) + 0.1 * (ib == 0)))
cfp = oancfisna * (ib - oancf_alt) / mve + oancfnotna * oancf / mve
print('ten done')
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
sgr = rate_of_change(revt)  # sale == revt
chpm = (ib / revt) - (lag(ib) / lag(revt))
print('ten done')
chato = 2 * (revt / (at + lag(at))) - (lag(revt) / (lag(at) + lag(at, by=2)))
pchsale_pchinvt = rate_of_change(revt) - rate_of_change(invt)
pchsale_pchrect = rate_of_change(revt) - rate_of_change(rect)
pchgm_pchsale = rate_of_change(revt - cogs) - rate_of_change(revt)
pchsale_pchxsga = rate_of_change(revt) - rate_of_change(xsga)
depr = dp / ppent
pchdepr = rate_of_change(depr)
chadv = log(1 + xad) - log(1 + lag(xad))
invest = (delta(ppegt.replace(0, float('NaN')).fillna(ppent)) + delta(invt)) / lag(at)
egr = rate_of_change(ceq)
print('ten done')
capx = capx.replace(0, float('NaN')).fillna(delta(ppent))
pchcapx = rate_of_change(capx)
grcapx = (capx - lag(capx, by=2)) / lag(capx, by=2)
grGW = rate_of_change(gdwlia)
grGW = (gdwlia == 0) * 0 + ((gdwlia != 0) * (grGW.isna())) * 1 + (grGW.notna()) * grGW  # legit isna/notna
woGW = (gdwlia != 0)
tang = (che + rect * 0.715 + invt * 0.547 + ppent * 0.535) / at
# if (2100<=sic<=2199) or (2080<=sic<=2085) or (naics in ('7132','71312','713210','71329','713290','72112','721120'))
#     then sin=1 else sin=0  # TODO
act = act.replace(0, float('NaN')).fillna(che + rect + invt)
lct = lct.replace(0, float('NaN')).fillna(ap)
currat = act / lct
print('ten done')
pchcurrat = rate_of_change(act / lct)
quick = (act - invt) / lct
pchquick = rate_of_change((act - invt) / lct)
salecash = revt / che
salerec = revt / rect
saleinv = revt / invt
pchsaleinv = rate_of_change(revt / invt)
cashdebt = 2 * (ib + dp) / (lt + lag(lt))
fatb = pd.DataFrame(0, columns=fatl.columns, index=fatl.index)  # TODO
realestate = (fatb + fatl) / (ppegt.replace(0, float('NaN')).fillna(ppent))
print('ten done')
divi = ((dvt > 0) * ((lag(dvt) == 0) + lag(dvt).isna())).astype(int)  # starts paying dividends, legit isna
divo = (((dvt == 0) + dvt.isna()) * (lag(dvt) > 0)).astype(int)  # stops paying dividends, legit isna
obklg = 2 * ob / (at + lag(at))
chobklg = delta(ob) / (at + lag(at)) * 2
securedind = (dm != 0).astype(int)
secured = dm / dltt
convind = ((dc != 0) + (csho > 0)).astype(int)
conv = dc / dltt
grltnoa = (delta(+ppent + intan + ao - lo) - dp) / (at + lag(at)) * 2
chdrc = delta(dr) / (at + lag(at)) * 2
print('ten done')
rd = (rate_of_change(xrd / at) > .05).astype(int)
roe = ib / lag(ceq)
rdbias = rate_of_change(xrd) - roe
operprof = (revt - cogs - xsga - xint) / lag(ceq)
ps = (ni > 0).astype(int) + (oancf > 0).astype(int) + (ni / at > lag(ni / at)).astype(int) + (oancf > ni).astype(
    int) + (dltt / at < lag(dltt / at)).astype(int) + (act / lct > lag(act / lct)).astype(int) + (
                 (revt - cogs) / revt > (lag((revt - cogs) / revt)).astype(int)) + (revt / at > lag(revt / at)).astype(
    int) + (scstkc == 0).astype(int)

# TODO: Lev and Nissim (2004) https://www.jstor.org/stable/4093085

roa = atlagat * ni
cfroa = atlagat * (oancf + (oancf == 0) * (ib + dp))
xrdint = atlagat * xrd
capxint = atlagat * capx
xadint = atlagat * xad
print('ten done')

bm.to_csv('./files/by_data/seventyeight/bm.csv')
ep.to_csv('./files/by_data/seventyeight/ep.csv')
cashpr.to_csv('./files/by_data/seventyeight/cashpr.csv')
dy.to_csv('./files/by_data/seventyeight/dy.csv')
lev.to_csv('./files/by_data/seventyeight/lev.csv')
sp.to_csv('./files/by_data/seventyeight/sp.csv')
roic.to_csv('./files/by_data/seventyeight/roic.csv')
agr.to_csv('./files/by_data/seventyeight/agr.csv')
gma.to_csv('./files/by_data/seventyeight/gma.csv')
chcsho.to_csv('./files/by_data/seventyeight/chcsho.csv')
lgr.to_csv('./files/by_data/seventyeight/lgr.csv')
acc.to_csv('./files/by_data/seventyeight/acc.csv')
pctacc.to_csv('./files/by_data/seventyeight/pctacc.csv')
cfp.to_csv('./files/by_data/seventyeight/cfp.csv')
absacc.to_csv('./files/by_data/seventyeight/absacc.csv')
age.to_csv('./files/by_data/seventyeight/age.csv')
chinv.to_csv('./files/by_data/seventyeight/chinv.csv')
hire.to_csv('./files/by_data/seventyeight/hire.csv')
sgr.to_csv('./files/by_data/seventyeight/sgr.csv')
pchsale_pchrect.to_csv('./files/by_data/seventyeight/pchsale_pchrect.csv')
pchgm_pchsale.to_csv('./files/by_data/seventyeight/pchgm_pchsale.csv')
depr.to_csv('./files/by_data/seventyeight/depr.csv')
pchdepr.to_csv('./files/by_data/seventyeight/pchdepr.csv')
invest.to_csv('./files/by_data/seventyeight/invest.csv')
egr.to_csv('./files/by_data/seventyeight/egr.csv')
tang.to_csv('./files/by_data/seventyeight/tang.csv')
# sin.to_csv('./files/by_data/seventyeight/sin.csv')
currat.to_csv('./files/by_data/seventyeight/currat.csv')
pchcurrat.to_csv('./files/by_data/seventyeight/pchcurrat.csv')
salecash.to_csv('./files/by_data/seventyeight/salecash.csv')
salerec.to_csv('./files/by_data/seventyeight/salerec.csv')
cashdebt.to_csv('./files/by_data/seventyeight/cashdebt.csv')
divi.to_csv('./files/by_data/seventyeight/divi.csv')
divo.to_csv('./files/by_data/seventyeight/divo.csv')
securedind.to_csv('./files/by_data/seventyeight/securedind.csv')
conv.to_csv('./files/by_data/seventyeight/conv.csv')
rd.to_csv('./files/by_data/seventyeight/rd.csv')
operprof.to_csv('./files/by_data/seventyeight/operprof.csv')
ps.to_csv('./files/by_data/seventyeight/ps.csv')
