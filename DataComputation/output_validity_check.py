import pandas as pd
import Misc.useful_stuff as us
from os.path import join as pathjoin

output_dir = '../data/processed/output_by_month/'
months = us.listdir(output_dir)
for amonth in months:
    df = pd.read_csv(pathjoin(output_dir, amonth)).set_index('Unnamed: 0')
    if 'age' in df.columns:
        df = df.drop('age', axis=1)
    print(f'For {amonth}...')
    print(f'No. of all-real companies: {(df.notna().sum(axis=1) == len(df.columns)).sum()}')
    print(f'Ttl no. of entities: {len(df.columns)}')
    print(f'Max no. of entities each company has: {df.notna().sum(axis=1).replace(0, float("NaN")).dropna().max()}')
    print(f'Avg no. of entities each company has: {df.notna().sum(axis=1).replace(0, float("NaN")).dropna().mean()}')
    print(f'Med no. of entities each company has: {df.notna().sum(axis=1).replace(0, float("NaN")).dropna().median()}')
    print(f'')
