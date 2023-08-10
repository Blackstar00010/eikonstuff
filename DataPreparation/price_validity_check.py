import pandas as pd
import Misc.useful_stuff as us

price_data_dir = '../data/price_stuff/adj_price_data_fixed/'

files = us.listdir(price_data_dir, file_type='csv')
duplicate_dateQ = True
if duplicate_dateQ:
    count = 1
    for afile in files:
        if afile == 'NDCM-L^C14.csv':
            afile = afile
        df = pd.read_csv(price_data_dir+afile)
        date_col_name = us.date_col_finder(df, df_name=afile)
        if df[date_col_name].duplicated().any():
            df = df.sort_values(by=[date_col_name, 'VOLUME'], ascending=[True, False])
            dup_bool = df[date_col_name].duplicated(keep=False)
            df = df.drop_duplicates(subset=date_col_name, keep='first')
            # print(f'{count}: Following indexes are duplicated for {afile}!')
            # print(dup_ind)
            print(afile)
            print(df)
            count += 1
            df.to_csv(price_data_dir+afile, index=False)

discontinuityQ = False
if discontinuityQ:
    count = 1
    for afile in files:
        df = pd.read_csv(price_data_dir+afile)
        df = df.set_index(us.date_col_finder(df, df_name=afile)).replace(0, float('NaN'))
        df = df.drop(columns=['COUNT', 'VOLUME']).dropna(how='all')
        ret_df = df/df.shift(1)
        bool_df = (ret_df > 2) + (ret_df < 0.5)
        bool_df = bool_df + bool_df.shift(1).fillna(False) + bool_df.shift(-1).fillna(False)
        problematic_dates = df.index[bool_df.any(axis=1)]
        if len(problematic_dates) > 0:
            print(f'\n{count}: {afile} looks problematic!')
            print(df.loc[problematic_dates, :])
            count += 1
