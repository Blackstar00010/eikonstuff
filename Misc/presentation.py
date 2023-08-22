import pandas as pd
import useful_stuff as us
import matplotlib.pyplot as plt
import datetime

pres_dir = '../data/_presentation/'
proc_wrds_dir = '../data/processed_wrds/'
proc_ref_dir = '../data/processed/'


def import_price(file_name: str, hat_in_cols: bool) -> pd.DataFrame:
    """
    
    :param file_name: 
    :return: 
    """
    df = pd.read_csv(file_name)
    date_col_name = us.date_col_finder(df, file_name)
    df[date_col_name] = us.datetime_to_str(df[date_col_name])
    df = df.sort_values(by=date_col_name)
    if df[date_col_name].duplicated().any():
        df = df.set_index(date_col_name)
        df = df[~df.index.duplicated(keep='first')].fillna(df[~df.index.duplicated(keep='last')])
        df = us.fillna(df, hat_in_cols=hat_in_cols)
    else:
        df = us.fillna(df, hat_in_cols=hat_in_cols)
        df = df.set_index(date_col_name)
    df = df.sort_index()
    df = df.replace([0, float('inf'), -float('inf')], float('NaN'))
    return df


def notna_per_row(df: pd.DataFrame) -> pd.Series:
    """
    Returns the number of non-NaN values per row in a DataFrame.
    :param df: the DataFrame
    :return:
    """
    return df.notna().sum(axis=1).replace(0, float('NaN')).fillna(method='ffill')


price_yf = import_price(pres_dir + 'price_yf.csv', hat_in_cols=False)
price_wrds = import_price(proc_wrds_dir + 'input_secd/close.csv', hat_in_cols=True)
price_ref = import_price(proc_ref_dir + 'input_secd/close.csv', hat_in_cols=True)

notna_counts_list = [notna_per_row(price_yf)[:-1], notna_per_row(price_wrds), notna_per_row(price_ref)]
for i in range(len(notna_counts_list)):
    notna_counts_df = pd.concat(notna_counts_list[:(i+1)], axis=1)
    notna_counts_df.columns = ['Yahoo Finance', 'WRDS', 'Refinitiv'][:(i+1)]
    notna_counts_df = notna_counts_df.sort_index()
    notna_counts_df = notna_counts_df.fillna(method='ffill')

    notna_counts_df.plot()
    plt.title('Number of stocks with price data')
    locs, labels = plt.xticks()
    labels = [label.get_text() for label in labels]
    label_df = pd.DataFrame({'locs': locs[1:-1], 'labels': pd.to_datetime(labels[1:-1])})
    for ind in label_df.index:
        if pd.isna(label_df.loc[ind, 'labels']):
            label_df.loc[ind, 'labels'] = (label_df.loc[ind-1, 'labels'] +
                                           datetime.timedelta(days=(label_df.loc[ind, 'locs'] -
                                                                    label_df.loc[ind-1, 'locs'])))
    plt.xticks(label_df['locs'], label_df['labels'].dt.strftime('%Y-%m'))
    plt.ylabel('Count')
    plt.savefig(pres_dir + f'price_data_count_{i}.png', dpi=300)
    # plt.show()

    (notna_counts_df / notna_counts_df.max(axis=0)).plot()
    plt.title('Normalised number of stocks with price data')
    plt.xticks(label_df['locs'], label_df['labels'].dt.strftime('%Y-%m'))
    plt.ylabel('%')
    plt.savefig(pres_dir + f'price_data_count_norm_{i}.png', dpi=300)
    # plt.show()
