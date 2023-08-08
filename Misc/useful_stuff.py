import platform
import os
import pandas as pd
from typing import Literal


def beep() -> None:
    """
    Makes a beep sound
    """
    if platform.system() == 'Darwin':
        import beepy
        beepy.beep()
    else:
        import winsound
        winsound.Beep(1000, 500)


def listdir(directory: str, file_type='csv') -> list:
    """
    Alternative for os.listdir.
    :param directory: the directory to find certain type of file.
    :param file_type: extension of data to find (e.g. 'csv' or 'pickle')
    :return: list of file names
    """
    ret = os.listdir(directory)
    ret = [item for item in ret if item.split('.')[-1] == file_type]
    return sorted(ret)


def date_col_finder(dataframe, df_name) -> str:
    """
    Finds the name of the column that contains date values.
    :param dataframe: the dataframe to find the desired column
    :param df_name: the name of the dataframe
    :return:
    """
    possible_date_list = ['datadate', 'DataDate', 'Datadate', 'Date', 'date']
    date_col = dataframe.columns[dataframe.columns.isin(possible_date_list)]

    if len(date_col) < 1:
        raise IndexError(f'No column named correctly is in {df_name}')
    elif len(date_col) > 1:
        date_col_list = date_col
        date_col = input(f'Which column among these is the column that contains date in the file {df_name}?\n'
                         f'{[item for item in date_col_list]}')
        while date_col not in date_col_list:
            date_col = input(
                f'Which column among these is the column that contains date in the file {df_name}?\n'
                f'{[item for item in date_col_list]}')
        date_col = date_col_list[date_col_list.isin([date_col])]
    date_col = date_col[0]
    return date_col


def outer_joined_axis(directory: str, file_type: Literal['csv', 'pickle'] = 'csv',
                      axis: Literal['row', 'daterow', 'column'] = 'daterow') -> pd.Series:
    """
    Returns outer-joined date of all the .csv data within the directory `dir`.
    :param directory: the directory to scan .csv data and apply the operation
    :param file_type: extension of data to find (e.g. 'csv' or 'pickle')
    :param axis: the axis to be joined and returned.
        'row' for rows except dates, 'daterow' for rows containing dates, and 'column' for columns(generally companies)
    :return: Series of row names
    """
    files = listdir(directory, file_type=file_type)
    ret = pd.Series()
    for afile in files:
        df = pd.read_csv(directory + afile)
        if 'row' in axis:
            ret = pd.Series(pd.concat([ret, df[date_col_finder(df, afile)]]))
        else:
            ret = pd.concat([ret, pd.Series(df.columns)])
        ret = ret.dropna().drop_duplicates().reset_index(drop=True)
        ret = datetime_to_str(ret) if axis == 'daterow' else ret
    ret = pd.Series(ret)
    ret = ret.sort_values()
    return ret


def give_same_axis(dir_or_dirs, file_type: Literal['csv', 'pickle'] = 'csv', minimum=pd.Series(),
                   axis: Literal['row', 'daterow', 'column'] = 'daterow', strip: bool = False,
                   strip_subset: list = None,
                   print_logs=True) -> pd.Series:
    """
    Outer-join all the dates or column titles of .csv data within the directory(ies) `dir_or_dirs`.
    :param dir_or_dirs: directory or list of directories to scan .csv data and apply the operation
    :param file_type: extension of data to find (e.g. 'csv' or 'pickle')
    :param minimum: minimum series of rows/columns each dataframe should have
    :param axis: the axis to be joined and returned
        'row' for any rows(indexes), 'daterow' for rows that contain dates,
         and 'column' for columns(generally companies).
        Hence, 'row'/'daterow' means vertical join and 'column' means horizontal join
    :param strip: strip first and last rows/columns if True
    :param strip_subset: the subset of index/columns to use when stripping
        each df's date/column-wo-date by default
    :param print_logs: prints logs if True
    :return: Series of row/column names (the same as outer_joined_date function)
    """
    # check if dir_or_dirs is in a right format
    if strip_subset is None:
        strip_subset = []
    if type(dir_or_dirs) == list:
        if len(dir_or_dirs) < 1:
            raise IndexError('The length of dir_or_dirs should be at least 1')
        elif len(dir_or_dirs) > 1:
            for adir in dir_or_dirs:
                minimum = pd.concat([minimum, outer_joined_axis(adir, file_type, axis)])
            minimum = minimum.dropna().drop_duplicates()
            for adir in dir_or_dirs:
                give_same_axis(adir, minimum=minimum)
            return minimum
        else:
            directory = dir_or_dirs[0]  # for comprehensiveness of the code below
    elif type(dir_or_dirs) == str:
        directory = dir_or_dirs  # for comprehensiveness of the code below
    else:
        raise TypeError('dir_or_dirs should be either list or str')

    axis_vector = pd.concat([minimum, outer_joined_axis(directory, file_type, axis)])
    if print_logs:
        print(f'\tFinished finding {"row" if axis == "column" else "column"} vector to apply! : \n{axis_vector}')

    files = listdir(directory, file_type=file_type)
    for afile in files:
        # todo: use multiprocessing
        df = pd.read_csv(directory + afile) if file_type == 'csv' else pd.read_pickle(directory + afile)
        date_col_name = date_col_finder(df, df_name=afile)

        if 'row' in axis:
            df = df.set_index(date_col_name)
            lacking_index = axis_vector[~axis_vector.isin(df.index)]
            to_concat = pd.DataFrame(float('NaN'), columns=df.columns, index=lacking_index)
            df = pd.concat([df, to_concat], axis=0)
            df = df.sort_index()
            if strip:
                if strip_subset is None:
                    df = strip_df(df, df.columns, axis='row')
                else:
                    df = strip_df(df, strip_subset, axis='row')
        else:
            lacking_index = axis_vector[~axis_vector.isin(df.columns)]
            to_concat = pd.DataFrame(float('NaN'), columns=lacking_index, index=df.index)
            df = pd.concat([df, to_concat], axis=1)
            if strip:
                if strip_subset is None:
                    df = strip_df(df, df.index, axis='column')
                else:
                    df = strip_df(df, strip_subset, axis='column')

        if print_logs:
            print(f'\t{axis} - {afile} finished!')

        if file_type == 'csv':
            df = df.reset_index(drop=False).rename(columns={'index': date_col_name}) if 'row' in axis else df
            df.to_csv(directory + afile, index=False)
        else:
            df.to_pickle(directory + afile)

    return axis_vector


def give_same_format(dir_or_dirs, file_type: Literal['csv', 'pickle'] = 'csv', ind_is_date=True,
                     minimum_index=pd.Series(), minimum_column=pd.Series(),
                     strip_rows=True, strip_cols=False, strip_rows_subset=None, strip_cols_subset=None,
                     print_logs=True) -> pd.DataFrame:
    """
    Outer-join all the dates AND column names of .csv data within the directory(ies) `dir_or_dirs`.
    :param dir_or_dirs: directory or list of directories to scan .csv data and apply the operation
    :param file_type: extension of data to find (e.g. 'csv' or 'pickle')
    :param ind_is_date: True if index (or the first column) contains date
    :param minimum_index: minimum series of dates each dataframe should have
    :param minimum_column: minimum series of column names each dataframe should have
    :param strip_rows: strips first and last rows of NaNs if True
    :param strip_cols: strips first and last columns of NaNs if True
    :param strip_rows_subset: the criterion for stripping rows
    :param strip_cols_subset: the criterion for stripping columns
    :param print_logs: prints logs if True
    :return: empty dataframe of the final index and column names
    """
    row_axis = 'daterow' if ind_is_date else 'row'
    ind = give_same_axis(dir_or_dirs=dir_or_dirs, file_type=file_type, minimum=minimum_index,
                         axis=row_axis, print_logs=print_logs, strip=strip_rows, strip_subset=strip_rows_subset)
    if print_logs:
        print('\tRows completed!')

    col = give_same_axis(dir_or_dirs=dir_or_dirs, file_type=file_type, minimum=minimum_column,
                         axis='column', print_logs=print_logs, strip=strip_cols, strip_subset=strip_cols_subset)
    if print_logs:
        print('\tColumns completed!')
    return pd.DataFrame(float('NaN'), columns=col, index=ind)


def strip_df(df: pd.DataFrame, subset: list, axis: Literal['row', 'column'] = 'row',
             method: Literal['both', 'first', 'last'] = 'both') -> pd.DataFrame:
    """
    Delete first nan rows/columns and last nan rows/columns. (how='all')
    :param df: the dataframe to strip
    :param subset: list of rows/columns to consider. automatically drops entities not existent in `df`'s columns/index
    :param axis: unit of the chunk that is going to be removed (e.g. if 'row', first and last rows are deleted)
    :param method: which part to strip.
        only the NaNs prior to the first valid data if 'first', only the NaNs after the last valid data if 'last',
        both for 'both'
    :return: stripped dataframe
    """
    subset = pd.Series(subset)
    if axis == 'row':
        subset = subset[subset.isin(df.columns)]
        mask = ~df.loc[:, subset].isna().all(axis=1)
        ff = mask.replace(False, True)
        bf = ff
        if method != 'last':
            ff = mask.replace(False, float('NaN')
                              ).fillna(method='ffill').replace(float('NaN'), False)  # first NaNs are False
        if method != 'first':
            bf = mask.replace(False, float('NaN')
                              ).fillna(method='backfill').replace(float('NaN'), False)  # last NaNs are False
        filtered_rows = df.index[ff * bf]
        filtered_cols = df.columns[:]
    else:
        subset = subset.isin(df.index)
        filtered_rows = df.index[:]
        mask = ~df.loc[subset, :].isna().all(axis=0)
        ff = mask.replace(False, True)
        bf = ff
        if method != 'last':
            ff = mask.replace(False, float('NaN')
                              ).fillna(method='ffill').replace(float('NaN'), False)  # first NaNs are False
        if method != 'first':
            bf = mask.replace(False, float('NaN')
                              ).fillna(method='backfill').replace(float('NaN'), False)  # last NaNs are False
        filtered_cols = df.columns[ff * bf]
    return df.loc[filtered_rows, filtered_cols]


def datetime_to_str(col: pd.Series) -> pd.Series:
    """
    Converts datetime Series into str Series.
    :param col: the column vector to convert to str in the format YYYY-MM-DD
    :return: pd.Series of the converted vector
    """
    col = pd.to_datetime(col, format='mixed').dt.strftime('%Y-%m-%d')
    if type(col[0]) != str:
        col = col.dt.strftime('%Y-%m-%d')
    return col


def zero_finder(df: pd.DataFrame, df_name: str, raise_error=False) -> pd.DataFrame:
    """
    Finds if 0 exists in the dataframe and prints where it is.
    :param df: the dataframe to search 0 in
    :param df_name: the name of the dataframe when printing error
    :param raise_error: raises ValueError if True
    :return: the smallest rectangular dataframe with zeros in it
    """
    iszero = df == 0
    ind = iszero.index[iszero.any(axis=1)]
    col = iszero.columns[iszero.any(axis=0)]
    ret = df.loc[ind, col]
    if ret.size > 0:
        if raise_error:
            raise ValueError(f'0 exists in {df_name}')
        print('dataframe with 0:', df_name, '\n', ret)
    return ret


def pivot(some_df: pd.DataFrame, df_name, col_col, row_col=None):
    """
    todo
    :param some_df: dataframe to pivot
    :param df_name: the name of the dataframe
    :param col_col: name of the column that contains the category to be used as the column of the output dataframe
    :param row_col: name of the column that contains the data to become index.
        uses `date_col_finder` function to find it by default
    :return: pivoted dataframe
    """
    if row_col is None:
        row_col = date_col_finder(some_df, df_name)
    value = some_df.columns.drop([row_col, col_col])
    ret = some_df.pivot_table(index=row_col, columns=col_col, values=value)
    ret.columns = ret.columns.droplevel(level=0)  # b/c of two-level column names
    return ret


if __name__ == '__main__':
    # give_same_format('data/temp/')
    beep()
