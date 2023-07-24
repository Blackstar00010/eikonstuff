import pandas as pd
import os


def fix_dates_df(from_df: pd.DataFrame, to_df: pd.DataFrame, from_col='datadate', to_col='datadate', fill='ffill'):
    """
    Reads date column from `from_df` and apply to `to_df`, fills NaNs depending on `fill` and return the fixed `to_df`.
    :param from_df: the df to bring the date column from
    :param to_df: the df to be changed
    :param from_col: name of the column that contains date values in `from_df`
    :param to_col: name of the column that contains date values in `to_df`
    :return: fixed `to_df`
    """
    date_col = from_df[from_col]
    to_df[to_col] = date_col
    if fill is not None:
        to_df.fillna(method=fill)
    return to_df



def fix_dates_csv(from_csv: str, to_csv: str, from_col='datadate', to_col='datadate', fill='ffill'):
    """
    Reads date column from `from_csv` and apply to `to_csv`, fills NaNs depending on `fill` and save as `to_csv`
    :param from_csv: the csv to bring the date column from
    :param to_csv: the csv to be changed
    :param from_col: name of the column that contains date values in `from_csv`
    :param to_col: name of the column that contains date values in `to_csv`
    :return: None
    """
    df1 = pd.read_csv(from_csv)
    df2 = pd.read_csv(to_csv)
    fix_dates_df(df1, df2, from_col, to_col)


if __name__ == '__main__':
