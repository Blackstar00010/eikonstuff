import pandas as pd


def fixer(dir_to_fix) -> pd.DataFrame:
    """
    Apply outer-joined columns and rows for all the .csv files within the given (list of) directory.
    :param dir_to_fix: Folder(s) that contain all the .csv files to fix. str or list
    :return: empty pd.DataFrame with final columns and rows(dates)
    """
    if type(dir_to_fix) not in [list, str]:
        raise TypeError('Type of dir_to_fix should be either str or list of str.')
    elif type(dir_to_fix) == list:
        for adir in dir_to_fix:
            fixer(adir)
    date_col = ['datadate', 'DataDate', 'Datadate', 'Date', 'date']
    cols = pd.DataFrame().columns
    rows = pd.Series()
    # TODO

    return pd.DataFrame(columns=cols, index=rows)


if __name__ == '__main__':
    print('hehe')

