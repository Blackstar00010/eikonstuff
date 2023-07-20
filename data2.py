import os
import pandas as pd
from numpy import log

date_col = pd.read_csv('files/metadata/business_days.csv')[['YYYY-MM-DD', 'YYYYMMDD']]


def date_fixer(target: pd.DataFrame, update=True):
    """

    :param target:
    :param update:
    :return:
    """
    target = target.merge(date_col)


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


def lag(dataframe: pd.DataFrame, date_col_name='datadate', by=1):
    """

    :param dataframe:
    :param date_col_name: the name of the column to use as date
    :return: a dataframe of calculated delta with the original date column
    """
    if date_col_name is None or date_col_name not in dataframe.columns:
        date_col_name = date_column_finder(dataframe)

    ret = dataframe.shift(periods=1)
    # TODO: annual
    # TODO: amount

    ret[date_col_name] = dataframe[date_col_name]
    return 0


def delta(dataframe: pd.DataFrame, date_col_name='datadate'):
    """
    Calculate absolute changes from the last different values vertically.
    :param dataframe: the dataframe to calculate delta. Must contain a column of dates.
    :param date_col_name: the name of the column to use as date
    :return: a dataframe of calculated delta with the original date column
    """
    if date_col_name is None or date_col_name not in dataframe.columns:
        date_col_name = date_column_finder(dataframe)

    ret = dataframe - dataframe.shift(periods=1)
    ret = ret.replace({0: float('NaN')})
    ret = ret.fillna(method='ffill')
    ret[date_col_name] = dataframe[date_col_name]
    return ret


def rate_of_change(dataframe: pd.DataFrame, date_col_name='datadate', minusone=True):
    """
    Calculate absolute changes from the last different values vertically. The value in the returned dataframe is 0.1 if
    the input dataframe's value changed from 10 to 11.
    :param dataframe: the dataframe to calculate delta. Must contain a column of dates.
    :param date_col_name: the name of the column to use as date.
    :param minusone: For a value changing from 10 to 11, the returned value is 0.1 if minusone=True and 1.1 if minusone=False. True by default.
    :return: a dataframe of calculated rate of change with the original date column
    """
    if date_col_name is None or date_col_name not in dataframe.columns:
        date_col_name = date_column_finder(dataframe)

    ret = delta(dataframe, date_col_name)
    ret = ret / (dataframe - ret)
    if not minusone:
        ret += 1
    ret[date_col_name] = dataframe[date_col_name]
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

dcpstk = data_df[data_df['data_name'] == 'dcpstk'].drop('data_name', axis=1)
dcvt = data_df[data_df['data_name'] == 'dcvt'].drop('data_name', axis=1)
if True:
    dm = data_df[data_df['data_name'] == 'dm'].drop('data_name', axis=1)
    drc = data_df[data_df['data_name'] == 'drc'].drop('data_name', axis=1)
    drlt = data_df[data_df['data_name'] == 'drlt'].drop('data_name', axis=1)
    gdwlia = data_df[data_df['data_name'] == 'gdwlia'].drop('data_name', axis=1)
    ni = data_df[data_df['data_name'] == 'ni'].drop('data_name', axis=1)
    ob = data_df[data_df['data_name'] == 'ob'].drop('data_name', axis=1)
    pstk = data_df[data_df['data_name'] == 'pstk'].drop('data_name', axis=1)
    pstkq = data_df[data_df['data_name'] == 'pstkq'].drop('data_name', axis=1)
    scstkc = data_df[data_df['data_name'] == 'scstkc'].drop('data_name', axis=1)
    txfed = data_df[data_df['data_name'] == 'txfed'].drop('data_name', axis=1)
    txt = data_df[data_df['data_name'] == 'txt'].drop('data_name', axis=1)
    xad = data_df[data_df['data_name'] == 'xad'].drop('data_name', axis=1)
    aco = data_df[data_df['data_name'] == 'aco'].drop('data_name', axis=1)
    act = data_df[data_df['data_name'] == 'act'].drop('data_name', axis=1)
    ao = data_df[data_df['data_name'] == 'ao'].drop('data_name', axis=1)
    ap = data_df[data_df['data_name'] == 'ap'].drop('data_name', axis=1)
    at = data_df[data_df['data_name'] == 'at'].drop('data_name', axis=1)
    capx = data_df[data_df['data_name'] == 'capx'].drop('data_name', axis=1)
    ceq = data_df[data_df['data_name'] == 'ceq'].drop('data_name', axis=1)
    che = data_df[data_df['data_name'] == 'che'].drop('data_name', axis=1)
    cogs = data_df[data_df['data_name'] == 'cogs'].drop('data_name', axis=1)
    dlc = data_df[data_df['data_name'] == 'dlc'].drop('data_name', axis=1)
    dltt = data_df[data_df['data_name'] == 'dltt'].drop('data_name', axis=1)
    dp = data_df[data_df['data_name'] == 'dp'].drop('data_name', axis=1)
    dvt = data_df[data_df['data_name'] == 'dvt'].drop('data_name', axis=1)
    ebit = data_df[data_df['data_name'] == 'ebit'].drop('data_name', axis=1)
    ebitda = data_df[data_df['data_name'] == 'ebitda'].drop('data_name', axis=1)
    fatb = data_df[data_df['data_name'] == 'fatb'].drop('data_name', axis=1)
    fatl = data_df[data_df['data_name'] == 'fatl'].drop('data_name', axis=1)
    gdwl = data_df[data_df['data_name'] == 'gdwl'].drop('data_name', axis=1)
    ib = data_df[data_df['data_name'] == 'ib'].drop('data_name', axis=1)
    intan = data_df[data_df['data_name'] == 'intan'].drop('data_name', axis=1)
    invt = data_df[data_df['data_name'] == 'invt'].drop('data_name', axis=1)
    lco = data_df[data_df['data_name'] == 'lco'].drop('data_name', axis=1)
    lct = data_df[data_df['data_name'] == 'lct'].drop('data_name', axis=1)
    lo = data_df[data_df['data_name'] == 'lo'].drop('data_name', axis=1)
    lt = data_df[data_df['data_name'] == 'lt'].drop('data_name', axis=1)
    nopi = data_df[data_df['data_name'] == 'nopi'].drop('data_name', axis=1)
    oancf = data_df[data_df['data_name'] == 'oancf'].drop('data_name', axis=1)
    pi = data_df[data_df['data_name'] == 'pi'].drop('data_name', axis=1)
    ppegt = data_df[data_df['data_name'] == 'ppegt'].drop('data_name', axis=1)
    ppent = data_df[data_df['data_name'] == 'ppent'].drop('data_name', axis=1)
    rect = data_df[data_df['data_name'] == 'rect'].drop('data_name', axis=1)
    revt = data_df[data_df['data_name'] == 'revt'].drop('data_name', axis=1)
    sale = data_df[data_df['data_name'] == 'sale'].drop('data_name', axis=1)
    spi = data_df[data_df['data_name'] == 'spi'].drop('data_name', axis=1)
    txdi = data_df[data_df['data_name'] == 'txdi'].drop('data_name', axis=1)
    txp = data_df[data_df['data_name'] == 'txp'].drop('data_name', axis=1)
    xint = data_df[data_df['data_name'] == 'xint'].drop('data_name', axis=1)
    xrd = data_df[data_df['data_name'] == 'xrd'].drop('data_name', axis=1)
    xsga = data_df[data_df['data_name'] == 'xsga'].drop('data_name', axis=1)

    mve = pd.read_csv(secd_dir + 'mve.csv')
    close = pd.read_csv(secd_dir + 'close.csv')
    csho = pd.read_csv(secd_dir + 'shrout.csv')

print('all data loaded.')

# TODO: notna, isna, fillna

bm = ceq / mve
ep = ib / mve
cashpr = ((mve + dltt - at) / che)
dy = dvt / mve
lev = lt / mve
sp = sale / mve
roic = (ebit - nopi) / (ceq + lt - che)
rd_sale = xrd / sale
rd_mve = xrd / mve
agr = rate_of_change(at)
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
absacc = abs(acc)
# age = count  # TODO
chinv = atlagat * delta(invt)
spii = (spi != 0) * spi.notna()
spi = atlagat * spi
cf = atlagat * (
        oancfnotna * oancf +
        oancfisna * (ib - oancf_alt))
# hire = rate_of_change(emp) * emp.notna() * lag(emp).notna()  # TODO
# if missing(emp) or missing(lag(emp)) then hire=0
sgr = rate_of_change(revt)  # sale == revt
chpm = (ib / revt) - (lag(ib) / lag(revt))
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
capx = capx.replace(0, float('NaN')).fillna(delta(ppent))
pchcapx = rate_of_change(capx)
grcapx = (capx - lag(capx, by=2)) / lag(capx, by=2)
grGW = rate_of_change(gdwl)
grGW = grGW.fillna((gdwl != 0) * (grGW.isna() + (grGW == 0)) * 1).astype(float)  # TODO: fillna & isna
# woGW = (gdwlia.notna()*(gdwlia!=0)) + # TODO
# if (not missing(gdwlia) and gdwlia ne 0) or (not missing(gdwlip) and gdwlip ne 0) or (not missing(gwo) and gwo ne 0) then woGW=1
#     else woGW=0
tang = (che + rect * 0.715 + invt * 0.547 + ppent * 0.535) / at
# if (2100<=sic<=2199) or (2080<=sic<=2085) or (naics in ('7132','71312','713210','71329','713290','72112','721120'))
#     then sin=1 else sin=0  # TODO
act = act.replace(0, float('NaN')).fillna(che + rect + invt)
lct = lct.replace(0, float('NaN')).fillna(ap)
currat = act / lct
pchcurrat = rate_of_change(act / lct)
quick = (act - invt) / lct
pchquick = rate_of_change((act - invt) / lct)
salecash = sale / che
salerec = sale / rect
saleinv = sale / invt
pchsaleinv = rate_of_change(revt / invt)
cashdebt = 2 * (ib + dp) / (lt + lag(lt))
realestate = (fatb + fatl) / (ppegt.replace(0, float('NaN')).fillna(ppent))
divi = ((dvt > 0) * ((lag(dvt) == 0) + lag(dvt).isna())).astype(int)  # starts paying dividends
divo = (((dvt == 0) + dvt.isna()) * (lag(dvt) > 0)).astype(int)  # stops paying dividends
obklg = 2 * ob / (at + lag(at))
chobklg = delta(ob) / (at + lag(at)) * 2
securedind = ((dm != 0) * dm.notna()).astype(int)
secured = dm / dltt
convind = (((dc != 0) * dc.notna()) + (cshrc > 0)).astype(int)
conv = dc / dltt
grltnoa = (delta(+ppent + intan + ao - lo) - dp) / (at + lag(at)) * 2
chdrc = delta(dr) / (at + lag(at)) * 2
rd = (rate_of_change(xrd / at) > .05).astype(int)
roe = ib / lag(ceq)
rdbias = rate_of_change(xrd) - roe
operprof = (revt - cogs - xsga0 - xint0) / lag(ceq)

bm.to_csv('./files/by_data/seventyeight/bm.csv')
ep.to_csv('./files/by_data/seventyeight/ep.csv')
cashpr.to_csv('./files/by_data/seventyeight/cashpr.csv')
dy.to_csv('./files/by_data/seventyeight/dy.csv')
lev.to_csv('./files/by_data/seventyeight/lev.csv')
sp.to_csv('./files/by_data/seventyeight/bm.csv')
roic.to_csv('./files/by_data/seventyeight/bm.csv')
agr.to_csv('./files/by_data/seventyeight/bm.csv')
gma.to_csv('./files/by_data/seventyeight/bm.csv')
chcsho.to_csv('./files/by_data/seventyeight/bm.csv')
lgr.to_csv('./files/by_data/seventyeight/bm.csv')
acc.to_csv('./files/by_data/seventyeight/bm.csv')
pctacc.to_csv('./files/by_data/seventyeight/bm.csv')
cfp.to_csv('./files/by_data/seventyeight/bm.csv')
absacc.to_csv('./files/by_data/seventyeight/bm.csv')
age.to_csv('./files/by_data/seventyeight/bm.csv')
chinv.to_csv('./files/by_data/seventyeight/bm.csv')
hire.to_csv('./files/by_data/seventyeight/bm.csv')
sgr.to_csv('./files/by_data/seventyeight/bm.csv')
pchsale_pchrect.to_csv('./files/by_data/seventyeight/bm.csv')
pchgm_pchsale.to_csv('./files/by_data/seventyeight/bm.csv')
depr.to_csv('./files/by_data/seventyeight/bm.csv')
pchdepr.to_csv('./files/by_data/seventyeight/bm.csv')
invest.to_csv('./files/by_data/seventyeight/bm.csv')
egr.to_csv('./files/by_data/seventyeight/bm.csv')
tang.to_csv('./files/by_data/seventyeight/bm.csv')
sin.to_csv('./files/by_data/seventyeight/bm.csv')
currat.to_csv('./files/by_data/seventyeight/bm.csv')
pchcurrat.to_csv('./files/by_data/seventyeight/bm.csv')
salecash.to_csv('./files/by_data/seventyeight/bm.csv')
salerec.to_csv('./files/by_data/seventyeight/bm.csv')
cashdebt.to_csv('./files/by_data/seventyeight/bm.csv')
divi.to_csv('./files/by_data/seventyeight/bm.csv')
divo.to_csv('./files/by_data/seventyeight/bm.csv')
securedind.to_csv('./files/by_data/seventyeight/bm.csv')
conv.to_csv('./files/by_data/seventyeight/bm.csv')
rd.to_csv('./files/by_data/seventyeight/bm.csv')
operprof.to_csv('./files/by_data/seventyeight/bm.csv')
ps.to_csv('./files/by_data/seventyeight/bm.csv')
