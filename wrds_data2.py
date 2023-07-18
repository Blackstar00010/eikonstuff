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


def delta(dataframe: pd.DataFrame, date_col_name=None):
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


def rate_of_change(dataframe: pd.DataFrame, date_col_name=None):
    """
    Calculate absolute changes from the last different values vertically.
    :param dataframe: the dataframe to calculate delta. Must contain a column of dates.
    :param date_col_name: the name of the column to use as date
    :return: a dataframe of calculated rate of change with the original date column
    """
    if date_col_name is None or date_col_name not in dataframe.columns:
        date_col_name = date_column_finder(dataframe)

    ret = delta(dataframe, date_col_name)
    ret = ret / (dataframe - ret)
    ret[date_col_name] = dataframe[date_col_name]
    return ret


def lag(dataframe, by='year'):
    """

    :param dataframe:
    :param by: int or str. 'year' or 'month'
    :return:
    """

    bus_days_df = pd.read_csv('files/metadata/business_days.csv')
    bus_days = {bus_days_df.loc[i, 'year']: bus_days_df.loc[i, 'business_days'] for i in range(len(bus_days_df))}
    ret = dataframe # TODO
    return ret


funda_dir = 'files/by_data/funda/'
ref_dir = 'files/by_data/from_ref/'

dcpstk = pd.read_csv('files/by_data/from_ref/dcpstk.csv')
dcvt = pd.read_csv('files/by_data/from_ref/dcvt.csv')
dm = pd.read_csv('files/by_data/from_ref/dm.csv')
drc = pd.read_csv('files/by_data/from_ref/drc.csv')
drlt = pd.read_csv('files/by_data/from_ref/drlt.csv')
gdwlia = pd.read_csv('files/by_data/from_ref/gdwlia.csv')
ni = pd.read_csv('files/by_data/from_ref/ni.csv')
ob = pd.read_csv('files/by_data/from_ref/ob.csv')
pstk = pd.read_csv('files/by_data/from_ref/pstk.csv')
pstkq = pd.read_csv('files/by_data/from_ref/pstkq.csv')
scstkc = pd.read_csv('files/by_data/from_ref/scstkc.csv')
txfed = pd.read_csv('files/by_data/from_ref/txfed.csv')
txt = pd.read_csv('files/by_data/from_ref/txt.csv')
xad = pd.read_csv('files/by_data/from_ref/xad.csv')



print('all data loaded.')
data_dict['bm'] = data_dict['ceq'] / data_dict['mve']
print('bm calculated.')
data_dict['ep'] = data_dict['ib'] / data_dict['mve']
print('ep calculated.')
data_dict['cashpr'] = (data_dict['mve'] + data_dict['dltt'] - data_dict['at']) / data_dict['che']
print('cashpr calculated.')
data_dict['dy'] = data_dict['dvt'] / data_dict['mve']
print('dy calculated.')
# TODO: just dps?
data_dict['lev'] = data_dict['lt'] / data_dict['mve']
print('lev calculated.')
data_dict['sp'] = data_dict['sale'] / data_dict['mve']
print('.')
data_dict['roic'] = (data_dict['ebit'] - data_dict['nopi']) / (data_dict['ceq'] + data_dict['lt'] - data_dict['che'])
# TODO: just fetch roic?
data_dict['sp'] = data_dict['sale'] / data_dict['mve']
data_dict['rd_sale'] = data_dict['xrd'] / data_dict['sale']
data_dict['rd_mve'] = data_dict['xrd'] / data_dict['mve']
data_dict['agr'] = rate_of_change(data_dict['at'], 'year') - 1
data_dict['gma'] = (data_dict['revt'] - data_dict['cogs']) / lag(data_dict['at'], 'year')
data_dict['chcsho'] = rate_of_change(data_dict['csho'], 'year') - 1
data_dict['lgr'] = rate_of_change(data_dict['lt'], 'year') - 1

oancf_notna = data_dict['oancf'].notna()
oancf_isna = data_dict['oancf'].isna()
data_dict['acc'] = oancf_notna * (data_dict['ib']-data_dict['oancf']) + \
                   oancf_isna * ((delta(data_dict['act']) - delta(data_dict['che'])) -
                                 (delta(data_dict['lct'])-delta(data_dict['dlc'])-delta(data_dict['txp'])-data_dict['dp']))
data_dict['acc'] = data_dict['acc'] / (data_dict['at'] + lag(data_dict['at'])) * 2
data_dict['pctacc'] = (data_dict['ib'] - data_dict['oancf']) / (oancf_notna * data_dict['ib'] + oancf_isna*0.01)

# data_dict['']