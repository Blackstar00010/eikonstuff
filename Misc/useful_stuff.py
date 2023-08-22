import platform
import os
import pandas as pd
from typing import Literal
import multiprocessing as mp
import numpy as np


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


def listdir(directory: str, file_type='csv', files_to_exclude=None) -> list:
    """
    Alternative for os.listdir.
    :param directory: the directory to find certain type of file.
    :param file_type: extension of data to find (e.g. 'csv', 'pickle', or 'dir').
        Can use None to make it behave the same as os.listdir without files starting with '.' or '_'
    :param files_to_exclude: list of files to exclude.
    :return: list of file names
    """
    if files_to_exclude is None:
        files_to_exclude = []
    ret = os.listdir(directory)
    to_drop = []
    if file_type is None:
        for afile in ret:
            if afile[0] in ['.', '_']:
                to_drop.append(afile)
            elif afile in files_to_exclude:
                to_drop.append(afile)

    elif file_type != 'dir':
        ret = [item for item in ret if item.split('.')[-1] == file_type]
        for afile in ret:
            if afile in files_to_exclude:
                to_drop.append(afile)
            if afile + file_type in files_to_exclude:
                to_drop.append(afile + file_type)
    else:
        ret = [item for item in ret if os.path.isdir(directory + item)]
        for afile in ret:
            if afile in files_to_exclude:
                to_drop.append(afile)

    for afile in to_drop:
        ret.remove(afile)
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


def apply_axis_2df(df: pd.DataFrame, vector, dfname, axis: Literal['row', 'daterow', 'column', 'col'] = 'daterow',
                   how: Literal['outer', 'inner'] = 'outer',
                   strip: bool = True, strip_subset: list = None) -> pd.DataFrame:
    """
    Applies the vector to the dataframe `df`. if `axis` is 'row', the vector is applied to the index of `df`.
    :param df: the dataframe to apply the vector to
    :param vector: the vector to apply to the dataframe. list or pd.Series
    :param dfname: the name of the dataframe
    :param axis: the direction along which the vector will be applied.
        If 'row' or 'daterow', the file may be extended vertically,
        and if 'column' or 'col', the file may be extended horizontally
    :param how: 'outer' or 'inner'. If 'outer', the file will be extended to include the vector.
    :param strip: if True, the first and last NaNs will be stripped from the dataframe.
    :param strip_subset: if not None, the df will be stripped according to the columns of the df.
    :return: the dataframe with the vector applied
    """
    if type(vector) not in [list, pd.Series]:
        raise TypeError('vector must be a list or pd.Series')
    elif type(vector) == list:
        vector = pd.Series(vector)

    date_col_name = date_col_finder(df, dfname)
    df[date_col_name] = datetime_to_str(df[date_col_name])
    df = df.set_index(date_col_name)
    if how == 'inner':
        df = df.drop(index=df.index[~df.index.isin(vector)])
    if 'row' in axis:
        to_concat = pd.DataFrame(
            float('NaN'), columns=df.columns, index=vector[~vector.isin(df.index)])
        concat_axis = 0
    else:
        to_concat = pd.DataFrame(
            float('NaN'), columns=vector[~vector.isin(df.columns)], index=df.index)
        concat_axis = 1
    df = pd.concat([df, to_concat], axis=concat_axis)
    if strip:
        if strip_subset is None:
            df = strip_df(df, axis=axis, subset=None)
        else:
            df = strip_df(df, subset=strip_subset, axis=axis)
    df = df.sort_index()
    return df


def outer_joined_axis(directory: str, file_type: Literal['csv', 'pickle'] = 'csv',
                      axis: Literal['row', 'daterow', 'column', 'col'] = 'daterow') -> pd.Series:
    """
    Returns outer-joined axis of all the files within the directory.
    :param directory: the directory to scan files and apply the operation
    :param file_type: the type of file to scan. 'csv' or 'pickle'
    :param axis: the axis to be joined and returned.
        'row' for rows except dates, 'daterow' for rows containing dates,
        and 'column'/'col' for columns(generally companies)
    :return: the outer-joined axis in pd.Series
    """
    files = listdir(directory, file_type=file_type)
    ret = pd.Series()
    for afile in files:
        df = pd.read_csv(directory + afile, low_memory=False)
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
    :param dir_or_dirs: the directory or list of directories to scan files and apply the operation
    :param file_type: the type of file to scan. 'csv' or 'pickle'
    :param minimum: minimum series of rows/columns each dataframe should have
    :param axis: the axis to be joined and returned
        'row' for any rows(indexes), 'daterow' for rows that contain dates,
         and 'column' for columns(generally companies).
        Hence, 'row'/'daterow' means vertical join and 'column' means horizontal join
    :param strip: if True, strip first and last rows/columns
    :param strip_subset: the subset of index/columns to use when stripping.
        Each df's date/column-wo-date by default
    :param print_logs: if True, print logs
    :return: the outer-joined axis in pd.Series (same as the return of outer_joined_axis)
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
        raise TypeError('dir_or_dirs must be a list or str')

    axis_vector = pd.concat([minimum, outer_joined_axis(directory, file_type, axis)])
    if print_logs:
        print(f'\tFinished finding {"row" if axis == "column" else "column"} vector to apply! : \n{axis_vector}')

    files = listdir(directory, file_type=file_type)
    for afile in files:
        # todo: use multiprocessing
        df = pd.read_csv(
            directory + afile) if file_type == 'csv' else pd.read_pickle(directory + afile)
        date_col_name = date_col_finder(df, df_name=afile)

        if 'row' in axis:
            df = df.set_index(date_col_name)
            lacking_index = axis_vector[~axis_vector.isin(df.index)]
            to_concat = pd.DataFrame(
                float('NaN'), columns=df.columns, index=lacking_index)
            df = pd.concat([df, to_concat], axis=0)
            df = df.sort_index()
            if strip:
                if strip_subset is None:
                    df = strip_df(df, df.columns, axis='row')
                else:
                    df = strip_df(df, strip_subset, axis='row')
        else:
            lacking_index = axis_vector[~axis_vector.isin(df.columns)]
            to_concat = pd.DataFrame(
                float('NaN'), columns=lacking_index, index=df.index)
            df = pd.concat([df, to_concat], axis=1)
            if strip:
                if strip_subset is None:
                    df = strip_df(df, df.index, axis='column')
                else:
                    df = strip_df(df, strip_subset, axis='column')

        if print_logs:
            print(f'\t{axis} - {afile} finished!')

        if file_type == 'csv':
            df = df.reset_index(drop=False).rename(
                columns={'index': date_col_name}) if 'row' in axis else df
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


def strip_df(df: pd.DataFrame, subset: list = None, axis: Literal['row', 'column', 'col'] = 'row',
             method: Literal['both', 'first', 'last'] = 'both') -> pd.DataFrame:
    """
    Delete first nan rows/columns and last nan rows/columns. (how='all')
    :param df: the dataframe to strip
    :param subset: list of rows/columns to consider. automatically drops entities not existent in `df`'s columns/index.
        if None, all rows/columns are considered.
    :param axis: unit of the chunk that is going to be removed (e.g. if 'row', first and last rows are deleted)
    :param method: which part to strip.
        only the NaNs prior to the first valid data if 'first', only the NaNs after the last valid data if 'last',
        both for 'both'
    :return: stripped dataframe
    """
    if axis == 'row':
        if subset is None:
            subset = df.columns
        else:
            subset = pd.Series(subset)
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
        if subset is None:
            subset = df.index
        else:
            subset = pd.Series(subset)
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
    if type(col.iloc[0]) in [int, np.int64]:
        col = col.astype(str)

    if type(col.iloc[0]) == str:
        if col.iloc[0].count(' ') > 0:
            col = col.str.split(' ').str[0]
        if col.iloc[0].count('/') > 0:
            col = col.str.replace('/', '-')
        if col.iloc[0].count('-') == 0:
            col = col.apply(lambda x: x[:4] + '-' + x[4:6] + '-' + x[6:])
        col = pd.to_datetime(col, format='mixed').dt.strftime('%Y-%m-%d')
    else:
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


def fillna(some_df: pd.DataFrame, hat_in_cols=False) -> pd.DataFrame:
    """
    df.fillna() with ffill and bfill
    :param some_df:
    :param hat_in_cols:
    :return:
    """
    ret = some_df.fillna(method='ffill')
    last_nans = some_df.fillna(method='bfill').isna()
    if hat_in_cols:
        listed = some_df.columns[~(some_df.columns.str.find('^') > 0)]
        last_nans.loc[:, listed] = False
    ret = ret * (~last_nans)
    return ret


def fix_save_df(some_df: pd.DataFrame, directory: str, filename: str, index_label=None) -> None:
    """
    Replace 0, inf, and -inf with NaN and save the dataframe as csv.
    :param some_df: the dataframe to save
    :param directory: the directory to save the dataframe
    :param filename: the name of the file to save
    :param index_label: the name of the index column
    :return: None
    """
    if filename[-4:] == '.csv':
        filename = filename[:-4]
    if index_label is None:
        some_df.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(directory + filename + '.csv')
    else:
        some_df.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(
            directory + filename + '.csv', index_label=index_label)


if __name__ == '__main__':
    # give_same_format('data/temp/')
    beep()
