import pandas as pd
import os

to_fix_dirs = ['files/by_data/from_ref', '']

date_col = pd.read_csv('files/metadata/business_days.csv')[['YYYY-MM-DD', 'YYYYMMDD']]


close_df = pd.read_csv('files/by_data/secd/close.csv')[
    ['datadate', 'AZN.L']]  # AZN is just a placeholder to keep df not series
close_df['YYYY'] = close_df['datadate'].apply(lambda x: str(x)[:4])
close_df['MM'] = close_df['datadate'].apply(lambda x: str(x)[4:6])
close_df['DD'] = close_df['datadate'].apply(lambda x: str(x)[-2:])
close_df['YYYYMMDD'] = close_df['datadate']
close_df['YYYY-MM-DD'] = close_df['YYYY'] + '-' + close_df['MM'] + '-' + close_df['DD']
close_df = close_df.drop(['AZN.L', 'datadate'], axis=1)
close_df.to_csv('files/metadata/business_days.csv', index=False)

for adir in to_fix_dirs:
    file_names = os.listdir(adir)
    for file_name in file_names:
        the_df = pd.read_csv(adir+file_name)
        is_dashed = the_df.loc[0, 'datadate'].count('-') == 2
        to_compare = 'YYYY-MM-DD' if is_dashed else 'YYYYMMDD'
        # TODO


