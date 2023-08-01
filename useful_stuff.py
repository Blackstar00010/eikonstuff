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
    :param file_type: extension of files to find (e.g. 'csv' or 'pickle')
    :return: list of file names
    """
    ret = os.listdir(directory)
    return [item for item in ret if item.split('.')[-1] == file_type]


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
                      axis: Literal['row', 'column'] = 'row') -> pd.Series:
    """
    Returns outer-joined date of all the .csv files within the directory `dir`.
    :param directory: the directory to scan .csv files and apply the operation
    :param file_type: extension of files to find (e.g. 'csv' or 'pickle')
    :param axis: the axis to be joined and returned.
        'row' for rows(generally dates) and 'column' for columns(generally companies)
    :return: Series of row names
    """
    files = listdir(directory, file_type=file_type)
    ret = pd.Series()
    for afile in files:
        df = pd.read_csv(directory + afile)
        if axis == 'row':
            ret = pd.concat([ret, df[date_col_finder(df, afile)]])
        else:
            ret = pd.concat([ret, pd.Series(df.columns)])
        ret = ret.dropna().drop_duplicates()
    ret = pd.Series(ret)
    ret = ret.sort_values()
    return ret


def give_same_axis(dir_or_dirs, file_type: Literal['csv', 'pickle'] = 'csv', minimum=pd.Series(),
                   axis: Literal['row', 'column'] = 'row', strip: bool = False, print_logs=True) -> pd.Series:
    """
    Outer-join all the dates or column titles of .csv files within the directory(ies) `dir_or_dirs`.
    :param dir_or_dirs: directory or list of directories to scan .csv files and apply the operation
    :param file_type: extension of files to find (e.g. 'csv' or 'pickle')
    :param minimum: minimum series of rows/columns each dataframe should have
    :param axis: the axis to be joined and returned
        'row' for rows(generally dates) and 'column' for columns(generally companies)
        Hence, 'row' means vertical join and 'column' means horizontal join
    :param strip: strip first and last row/column if True.
    :param print_logs: prints logs if True
    :return: Series of row/column names (the same as outer_joined_date function)
    """
    # checkinf if dir_or_dirs is in a right format
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
        print(f'\tFinished finding {axis} vector to apply!')

    files = listdir(directory, file_type=file_type)
    for afile in files:
        df = pd.read_csv(directory + afile) if file_type == 'csv' else pd.read_pickle(directory + afile)
        date_col_name = date_col_finder(df, df_name=afile)

        if axis == 'row':
            df = df.set_index(date_col_name)
            lacking_index = axis_vector[~axis_vector.isin(df.index)]
            to_concat = pd.DataFrame(float('NaN'), columns=df.columns, index=lacking_index)
            df = pd.concat([df, to_concat], axis=0)
            df = df.sort_index()
            if strip:
                df = strip_df(df, df.columns.drop('Date'), axis='row')
        else:
            lacking_index = axis_vector[~axis_vector.isin(df.columns)]
            to_concat = pd.DataFrame(float('NaN'), columns=lacking_index, index=df.index)
            df = pd.concat([df, to_concat], axis=1)
            if strip:
                df = strip_df(df, df.index, axis='column')

        if print_logs:
            print(f'\t{axis} - {afile} finished!')

        if file_type == 'csv':
            df = df.reset_index(drop=False).rename(columns={'index': date_col_name}) if axis == 'row' else df
            df.to_csv(directory + afile, index=False)
        else:
            df.to_pickle(directory + afile)

    return axis_vector


def give_same_format(dir_or_dirs, file_type: Literal['csv', 'pickle'] = 'csv',
                     minimum_index=pd.Series(), minimum_column=pd.Series(),
                     strip=True, print_logs=True) -> pd.DataFrame:
    """
    Outer-join all the dates AND column names of .csv files within the directory(ies) `dir_or_dirs`.
    :param dir_or_dirs: directory or list of directories to scan .csv files and apply the operation
    :param file_type: extension of files to find (e.g. 'csv' or 'pickle')
    :param minimum_index: minimum series of dates each dataframe should have
    :param minimum_column: minimum series of column names each dataframe should have
    :param strip: strips first and last rows of NaNs if True
    :param print_logs: prints logs if True
    :return: empty dataframe of the final index and column names
    """
    ind = give_same_axis(dir_or_dirs=dir_or_dirs, file_type=file_type, minimum=minimum_index,
                         axis='row', print_logs=print_logs, strip=strip)
    if print_logs:
        print('\tRows completed!')
    col = give_same_axis(dir_or_dirs=dir_or_dirs, file_type=file_type, minimum=minimum_column,
                         axis='column', print_logs=print_logs, strip=strip)
    if print_logs:
        print('\tColumns completed!')
    return pd.DataFrame(float('NaN'), columns=col, index=ind)


def strip_df(df: pd.DataFrame, subset: list, axis: Literal['row', 'column'] = 'row') -> pd.DataFrame:
    """
    Delete first nan rows/columns and last nan rows/columns. (how='all')
    :param df: the dataframe to strip
    :param subset: list of rows/columns to consider. automatically drops entities not existent in `df`'s columns/index
    :param axis: unit of the chunk that is going to be removed (e.g. if 'row', first and last rows are deleted)
    :return: stripped dataframe
    """
    subset = pd.Series(subset)
    if axis == 'row':
        subset = subset.isin(df.columns)
        filtered_rows = df.index[~df.loc[:, subset].isna().all(axis=1)]
        first_row, last_row = filtered_rows[[0, -1]]
        filtered_rows = df.index[first_row: last_row]
        filtered_cols = df.columns[:]
    else:
        subset = subset.isin(df.index)
        filtered_rows = df.index[:]
        filtered_cols = df.columns[~df.loc[subset, :].isna().all(axis=0)]
        first_col, last_col = filtered_cols[[0, -1]]
        filtered_cols = df.columns[first_col: last_col]
    return df.loc[filtered_rows, filtered_cols]


def datetime_to_str(col: pd.Series) -> pd.Series:
    """
    Converts datetime Series into str Series.
    :param col: the column vector to convert to str in the format YYYY-MM-DD
    :return: pd.Series of the converted vector
    """
    col = pd.to_datetime(col).dt.strftime('%Y-%m-%d')
    if type(col[0]) != str:
        col = col.dt.strftime('%Y-%m-%d')
    return col


if __name__ == '__main__':
    # give_same_format('files/temp/')
    beep()
