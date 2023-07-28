import pandas as pd
from numpy import sqrt


def lag(dataframe: pd.DataFrame, by=1) -> pd.DataFrame:
    """

    :param dataframe:
    :param by: the amount of unit period to lag. (e.g. by=1 for 1 year if the data is FY0 type)
    :return: pd.DataFrame of calculated delta with the original date column
    """
    ischanged = (dataframe == dataframe.shift(periods=1))
    ret = dataframe.shift(periods=1) * ischanged
    ret = ret.fillna(method='ffill')
    if by > 1:
        ret = lag(ret, by=by-1)
    return ret


def delta(dataframe: pd.DataFrame, by=1) -> pd.DataFrame:
    """
    Calculate absolute changes from the last different values vertically. Simply, df-lag(df)
    :param dataframe: the dataframe to calculate delta. The column of dates must be the index column.
    :param by: the amount of unit period to calculate. (e.g. by=1 for 1 year if the data is FY0 type)
    :return: pd.DataFrame of calculated delta with the original date column
    """
    return dataframe - lag(dataframe, by=by)


def rate_of_change(dataframe: pd.DataFrame, by=1, minusone=True) -> pd.DataFrame:
    """
    Calculate absolute changes from the last different values vertically. The value in the returned dataframe is 0.1 if
    the input dataframe's value changed from 10 to 11. Simply, (df-lag(df))/lag(df)
    :param dataframe: the dataframe to calculate delta. Must contain a column of dates.
    :param by: the amount of unit period to calculate. (e.g. by=1 for 1 year if the data is FY0 type)
    :param minusone: For a value changing from 10 to 11, the returned value is 0.1 if minusone=True and 1.1 if minusone=False. True by default.
    :return: pd.DataFrame of calculated rate of change with the original date column
    """
    ret = delta(dataframe, by=by)
    ret = ret / (dataframe - ret)
    if not minusone:
        ret += 1
    return ret


def series_to_df(series: pd.Series, columns) -> pd.DataFrame:
    """
    Stretches horizontally the column vector so that the resulting dataframe has all the columns of `dataframe` with
    each row containing one value.
    :param series: the pd.Series to expand
    :param columns: list of names of the columns of the dataframe to be returned
    :return: pd.DataFrame
    """
    ret = pd.DataFrame(series)
    for acol in columns:
        ret[acol] = series
    ret = ret.drop(0, axis='columns')
    return ret


def ind_adj(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate industry-adjusted values. Specifically, it subtracts from rows their average
    :param dataframe: the dataframe to calculate industry-adjusted values
    :return: pd.DataFrame
    """
    # not using mean as we might have 0's instead of NaN's
    sum_row = dataframe.sum(axis=1)
    count_row = dataframe.notna().sum(axis=1)
    ind_avg = series_to_df(sum_row / count_row, dataframe.columns)
    return dataframe / ind_avg


def mean(list_of_df: list) -> pd.DataFrame:
    """
    Calculates mean of each element, excluding NaNs.
    :param list_of_df: list of dataframes. They all must have the same columns and rows.
    :return: pd.DataFrame containing mean values of each element
    """
    total = pd.DataFrame()
    count = pd.DataFrame()
    for df in list_of_df:
        total += df.fillna(0)
        count += df.notna() * 1
    return total / count


def stddev(list_of_df: list) -> pd.DataFrame:
    """
    Calculates standard deviation of each element, excluding NaNs.
    :param list_of_df: list of dataframes. They all must have the same columns and rows.
    :return: pd.DataFrame containing standard deviation values of each element
    """
    avg = mean(list_of_df)
    variance = 0
    for df in list_of_df:
        variance += (df - avg) ** 2
    return sqrt(variance)


def past_mean(df: pd.DataFrame, period=4) -> pd.DataFrame:
    """
    Calculates past `period` period mean of `df`.
    :param df: the dataframe to calculate past mean of
    :param period: the number of period to calculate past stddev. N if you want to calculate (N-1)th to current period's value.
    :return: pd.DataFrame of mean of each cell
    """
    the_list = [df]
    for i in range(1, period):
        the_list.append(lag(df, by=i))
    return mean(the_list)


def past_stddev(df: pd.DataFrame, period=4) -> pd.DataFrame:
    """
    Calculates past `period` period standard deviation of `df`.
    :param df: the dataframe to calculate past stddev of
    :param period: the number of period to calculate past stddev. N if you want to calculate (N-1)th to current period's value.
    :return: pd.DataFrame of standard deviation of
    """
    the_list = [df]
    for i in range(1, period):
        the_list.append(lag(df, by=i))
    return stddev(the_list)

