import pandas as pd
import useful_stuff as us
import matplotlib.pyplot as plt
import datetime

pres_dir = '../data/_presentation/'
proc_wrds_dir = '../data/processed_wrds/'
proc_ref_dir = '../data/price_stuff/price_data_merged/'


def import_csv(file_name: str, hat_in_cols: bool) -> pd.DataFrame:
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
        df = df.replace([0, float('inf'), -float('inf')], float('NaN'))
        df = df[~df.index.duplicated(keep='first')].fillna(df[~df.index.duplicated(keep='last')])
        df = us.fillna(df, hat_in_cols=hat_in_cols)
    else:
        df = df.replace([0, float('inf'), -float('inf')], float('NaN'))
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


price_yf = import_csv(pres_dir + 'price_yf.csv', hat_in_cols=False)
price_wrds = import_csv(proc_wrds_dir + 'input_secd/close.csv', hat_in_cols=True)
price_ref = import_csv(proc_ref_dir + 'close.csv', hat_in_cols=True)

notna_counts_list = [notna_per_row(price_yf)[:-1], notna_per_row(price_ref), notna_per_row(price_wrds)]
for i in range(len(notna_counts_list)):
    notna_counts_df = pd.concat(notna_counts_list[:(i + 1)], axis=1)
    notna_counts_df.columns = ['Yahoo Finance', 'Refinitiv', 'WRDS'][:(i + 1)]
    notna_counts_df = notna_counts_df.sort_index()
    notna_counts_df = notna_counts_df.fillna(method='ffill')

    notna_counts_df.plot()
    plt.title('Number of stocks with price data')
    locs, labels = plt.xticks()
    labels = [label.get_text() for label in labels]
    label_df = pd.DataFrame({'locs': locs[1:-1], 'labels': pd.to_datetime(labels[1:-1])})
    for ind in label_df.index:
        if pd.isna(label_df.loc[ind, 'labels']):
            label_df.loc[ind, 'labels'] = (label_df.loc[ind - 1, 'labels'] +
                                           datetime.timedelta(days=int(7 / 5 * (label_df.loc[ind, 'locs'] -
                                                                                label_df.loc[ind - 1, 'locs']))))
    plt.xticks(label_df['locs'], label_df['labels'].dt.strftime('%Y-%m'))
    plt.ylabel('Count')
    plt.savefig(pres_dir + f'price_data_count_{i}.png', dpi=300)
    # plt.show()

    (notna_counts_df / notna_counts_df.max(axis=0)).plot()
    plt.title('Normalised number of stocks with price data')
    plt.xticks(label_df['locs'], label_df['labels'].dt.strftime('%Y-%m'))
    plt.ylabel('%')
    plt.savefig(pres_dir + f'img_price/price_data_count_norm_{i}.png', dpi=300)
    # plt.show()

target_dir = '../data/processed_wrds/output_by_var_dd/'
vars = us.listdir(target_dir)
overall_nonzero = 0
overall_cells = 0
for avar in vars:
    df = import_csv(target_dir + avar, hat_in_cols=True)
    avar = avar.replace('.csv', '')

    total_cells = df.shape[0] * df.shape[1]
    total_nonzero = ((df != 0) * (df.notna())).sum().sum()
    print(f'\tNon-zero cells of {avar} : '
          f'{total_nonzero} / {total_cells} = {round(total_nonzero / (total_cells + 1) * 100, 2)}%')
    overall_nonzero += total_nonzero
    overall_cells += total_cells

    price_ref_temp = price_ref.loc[
        price_ref.index[price_ref.index.isin(df.index)], price_ref.columns[price_ref.columns.isin(df.columns)]]
    df = df.loc[df.index[df.index.isin(price_ref_temp.index)], df.columns[df.columns.isin(price_ref_temp.columns)]]
    pd.DataFrame([price_ref_temp.notna().sum(axis=1), df.notna().sum(axis=1)]).T.plot()
    plt.title(f'Number of non-empty cells of {avar}')
    plt.legend(['price', avar])
    plt.savefig(pres_dir + f'img_fund/nonempty_{avar}.png', dpi=300)
    plt.show()

print(
    f'Overall non-empty cells: {overall_nonzero} / {overall_cells} = {round(overall_nonzero / overall_cells * 100, 2)}%')
