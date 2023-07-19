import os
import pandas as pd


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


def my_abs(dataframe: pd.DataFrame, date_col_name='datadate'):
    """
    abs() function, but without the date column
    :param dataframe: the dataframe to calculate absolute function
    :param date_col_name: the name of the column that stores date values
    :return: a dataframe with abs() function applied
    """
    # TODO
    return 0


def my_log(dataframe: pd.DataFrame, date_col_name='datadate'):
    """
    abs() function, but without the date column
    :param dataframe: the dataframe to calculate absolute function
    :param date_col_name: the name of the column that stores date values
    :return: a dataframe with abs() function applied
    """
    # TODO
    return 0


funda_dir = 'files/by_data/funda/'
ref_dir = 'files/by_data/from_ref/'
secd_dir = 'files/by_data/secd/'

if True:
    dcpstk = pd.read_csv(ref_dir + 'dcpstk.csv')
    dcvt = pd.read_csv(ref_dir + 'dcvt.csv')
    dm = pd.read_csv(ref_dir + 'dm.csv')
    drc = pd.read_csv(ref_dir + 'drc.csv')
    drlt = pd.read_csv(ref_dir + 'drlt.csv')
    gdwlia = pd.read_csv(ref_dir + 'gdwlia.csv')
    ni = pd.read_csv(ref_dir + 'ni.csv')
    ob = pd.read_csv(ref_dir + 'ob.csv')
    pstk = pd.read_csv(ref_dir + 'pstk.csv')
    pstkq = pd.read_csv(ref_dir + 'pstkq.csv')
    scstkc = pd.read_csv(ref_dir + 'scstkc.csv')
    txfed = pd.read_csv(ref_dir + 'txfed.csv')
    txt = pd.read_csv(ref_dir + 'txt.csv')
    xad = pd.read_csv(ref_dir + 'xad.csv')
    aco = pd.read_csv(funda_dir + 'aco.csv')
    act = pd.read_csv(funda_dir + 'act.csv')
    ao = pd.read_csv(funda_dir + 'ao.csv')
    ap = pd.read_csv(funda_dir + 'ap.csv')
    at = pd.read_csv(funda_dir + 'at.csv')
    capx = pd.read_csv(funda_dir + 'capx.csv')
    ceq = pd.read_csv(funda_dir + 'ceq.csv')
    che = pd.read_csv(funda_dir + 'che.csv')
    cogs = pd.read_csv(funda_dir + 'cogs.csv')
    dlc = pd.read_csv(funda_dir + 'dlc.csv')
    dltt = pd.read_csv(funda_dir + 'dltt.csv')
    dp = pd.read_csv(funda_dir + 'dp.csv')
    dvt = pd.read_csv(funda_dir + 'dvt.csv')
    ebit = pd.read_csv(funda_dir + 'ebit.csv')
    ebitda = pd.read_csv(funda_dir + 'ebitda.csv')
    fatb = pd.read_csv(funda_dir + 'fatb.csv')
    fatl = pd.read_csv(funda_dir + 'fatl.csv')
    gdwl = pd.read_csv(funda_dir + 'gdwl.csv')
    ib = pd.read_csv(funda_dir + 'ib.csv')
    intan = pd.read_csv(funda_dir + 'intan.csv')
    invt = pd.read_csv(funda_dir + 'invt.csv')
    lco = pd.read_csv(funda_dir + 'lco.csv')
    lct = pd.read_csv(funda_dir + 'lct.csv')
    lo = pd.read_csv(funda_dir + 'lo.csv')
    lt = pd.read_csv(funda_dir + 'lt.csv')
    nopi = pd.read_csv(funda_dir + 'nopi.csv')
    oancf = pd.read_csv(funda_dir + 'oancf.csv')
    pi = pd.read_csv(funda_dir + 'pi.csv')
    ppegt = pd.read_csv(funda_dir + 'ppegt.csv')
    ppent = pd.read_csv(funda_dir + 'ppent.csv')
    rect = pd.read_csv(funda_dir + 'rect.csv')
    revt = pd.read_csv(funda_dir + 'revt.csv')
    sale = pd.read_csv(funda_dir + 'sale.csv')
    spi = pd.read_csv(funda_dir + 'spi.csv')
    txdi = pd.read_csv(funda_dir + 'txdi.csv')
    txp = pd.read_csv(funda_dir + 'txp.csv')
    xint = pd.read_csv(funda_dir + 'xint.csv')
    xrd = pd.read_csv(funda_dir + 'xrd.csv')
    xsga = pd.read_csv(funda_dir + 'xsga.csv')
    mve = pd.read_csv(secd_dir + 'mve.csv')
    close = pd.read_csv(secd_dir + 'close.csv')

print('all data loaded.')

# TODO: date column 빼고 나머지만으로 정의하기
# TODO: 1+NaN = NaN

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

oancfisna = oancf.isna()
oancfnotna = ~oancfisna
oancf_alt = delta(act) - delta(che) - delta(lct) + delta(dlc) + delta(txp) + dp
acc = 2 / (at + lag(at)) * (
        oancfnotna * (ib - oancf) +
        oancfisna * oancf_alt)
pctacc = (oancfnotna * (ib - oancf) / (my_abs(ib) * (ib != 0) + 0.1 * (ib == 0)) +
          oancfisna * oancf_alt / (my_abs(ib) * (ib != 0) + 0.1 * (ib == 0)))
cfp = oancfisna * (ib - oancf_alt) / mve + oancfnotna * oancf / mve
absacc = my_abs(acc)
# age = count  # TODO
chinv = 2 / (at + lag(at)) * delta(invt)
spii = (spi != 0) * spi.notna()
spi = 2 / (at + lag(at)) * spi
cf = 2 / (at + lag(at)) * (
        oancfnotna * oancf +
        oancfisna * (ib - oancf_alt))
hire = rate_of_change(emp) * emp.notna() * lag(emp).notna()  # TODO
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
chadv = my_log(1 + xad) - my_log(1 + lag(xad))
invest = (delta(ppegt.fillna(ppent)) + delta(invt)) / lag(at)
egr=rate_of_change(ceq)
capx = capx.fillna(delta(ppent))
pchcapx= rate_of_change(capx)
grcapx=(capx-lag(capx, by=2))/lag(capx, by=2)
grGW=rate_of_change(gdwl)
grGW = (gdwl!=0)*gdwl.notna()*grGW.isna()*1 + grgW.notna()*grGW  #TODO : fillna?
# woGW = (gdwlia.notna()*(gdwlia!=0)) + # TODO
# if (not missing(gdwlia) and gdwlia ne 0) or (not missing(gdwlip) and gdwlip ne 0) or (not missing(gwo) and gwo ne 0) then woGW=1
#     else woGW=0
tang=(che+rect*0.715+invt*0.547+ppent*0.535)/at					
if (2100<=sic<=2199) or (2080<=sic<=2085) or (naics in ('7132','71312','713210','71329','713290','72112','721120'))
    then sin=1 else sin=0
act = act.fillna(che+rect+invt)
lct = lct.fillna(ap)
currat=act/lct
pchcurrat=rate_of_change(act/lct)
quick=(act-invt)/lct
pchquick=rate_of_change((act-invt)/lct)
salecash=sale/che
salerec=sale/rect
saleinv=sale/invt
pchsaleinv= rate_of_change(revt/invt)
cashdebt=2*(ib+dp)/(lt+lag(lt))
realestate=(fatb+fatl)/(ppegt.fillna(ppent))
divi = ((dvt>0) * ((lag(dvt)==0) + lag(dvt).isna())).astype(int)  # starts paying dividends
divo = (((dvt==0)+dvt.isna()) * (lag(dvt)>0)).astype(int)  # stops paying dividends
obklg=2*ob/(at+lag(at))
chobklg=delta(ob)/(at+lag(at))*2
securedind = ((dm!=0) * dm.notna()).astype(int)
secured=dm/dltt
convind = (((dc!=0) * dc.notna()) + (cshrc>0)).astype(int)
conv=dc/dltt
grltnoa= ( delta(+ppent+intan+ao-lo) - dp ) / (at+lag(at)) * 2
chdrc=delta(dr)/(at+lag(at)) * 2
rd = (rate_of_change(xrd/at)>.05).astype(int)
roe=ib/lag(ceq)
rdbias=rate_of_change(xrd) - roe
operprof = (revt-cogs-xsga0-xint0)/lag(ceq)

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


