import numpy as np
import pandas as pd
import Misc.useful_stuff as us
from os.path import join as pathjoin
import _options as opt
import matplotlib.pyplot as plt

check_by_var = False
if check_by_var:
    target_dir = opt.by_var_mm_dir
    vars = us.listdir(target_dir)
    overall_realno = 0
    overall_nonzero = 0
    overall_cells = 0
    for avar in vars:
        df = pd.read_csv(pathjoin(target_dir, avar))
        df = df.set_index(us.date_col_finder(df, avar))
        total_realno = df.notna().sum().sum()
        total_cells = df.shape[0] * df.shape[1]
        total_nonzero = ((df!=0) * (df.notna())).sum().sum()
        print(f'Non-empty cells of {avar} : {total_realno} / {total_cells} = {round(total_realno / total_cells * 100, 2)}%')
        print(f'\tNon-zero cells of {avar} : {total_nonzero} / {total_realno} = {round(total_nonzero / (total_realno+1) * 100, 2)}%')
        overall_realno += total_realno
        overall_nonzero += total_nonzero
        overall_cells += total_cells
    print(f'Overall non-empty cells: {overall_realno} / {overall_cells} = {round(overall_realno / overall_cells * 100, 2)}%')
    print(f'\tOverall non-zero cells: {overall_nonzero} / {overall_realno} = {round(overall_nonzero / (overall_realno+1) * 100, 2)}%')

check_by_month = False
if check_by_month:
    target_dir = opt.output_dir
    months = us.listdir(target_dir)
    for amonth in months:
        if amonth[0] not in ['1', '2']:
            continue
        df = pd.read_csv(pathjoin(target_dir, amonth))
        if 'age' in df.columns:
            df = df.drop('age', axis=1)
        print(f'For {amonth}...')
        print(f'No. of all-real companies: {(df.notna().sum(axis=1) == len(df.columns)).sum()}')
        print(f'Ttl no. of entities: {len(df.columns)}')
        print(f'Max no. of entities each company has: {df.notna().sum(axis=1).replace(0, float("NaN")).dropna().max()}')
        print(f'Avg no. of entities each company has: {df.notna().sum(axis=1).replace(0, float("NaN")).dropna().mean()}')
        print(f'Med no. of entities each company has: {df.notna().sum(axis=1).replace(0, float("NaN")).dropna().median()}')
        print(f'')

check_momentum = True
if check_momentum:
    momentum_df = pd.read_csv(pathjoin(opt.output_dir, 'monthly_return.csv'))
    momentum_df = momentum_df.set_index(us.date_col_finder(momentum_df, 'monthly_return')) + 1
    momentum_df = momentum_df.apply(np.log).abs()
    momentum_df = momentum_df.replace(float('inf'), float('NaN'))
    max_mom_ser = momentum_df.max(axis=1)
    max_mom_ser.plot()
    plt.legend()
    plt.title('max(abs(log(return))) for each month')
    plt.show()
