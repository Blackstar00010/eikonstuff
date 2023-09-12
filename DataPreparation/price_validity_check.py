import pandas as pd
import Misc.useful_stuff as us
import DataComputation._options as opt
import matplotlib.pyplot as plt
import numpy as np
import Misc.plots as plots

secd_dir = opt.secd_dir
all_dir = opt.all_dir

price_data_dir = '../data/price_stuff/adj_price_data_fixed/'
files = us.listdir(price_data_dir)

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

mom_anomaly_check = True
export_shits = True
if mom_anomaly_check:
    for var_to_check in ['mve', 'close']:
        close_df = pd.read_csv(f'../data/preprocessed_wrds/price/{var_to_check}.csv')
        close_df = close_df.set_index(us.date_col_finder(close_df, var_to_check))
        # close_df = us.fillna(close_df, 'ffill')
        ret_df = close_df / close_df.shift(1)
        ret_df = ret_df.astype(float).apply(np.log).abs().replace(float('inf'), float('NaN'))

        top_shits = ret_df.max().sort_values(ascending=False)
        top_shits = top_shits[top_shits > np.log(10)]
        top_shits = top_shits.index

        to_check = 1000
        try:
            print(top_shits[min(to_check-1, len(top_shits)-1)])
        except IndexError:
            print('No more to check!')

        close_df.loc[:, top_shits[:to_check]].plot()
        plt.title(var_to_check)
        plt.show()
        (close_df.loc[:, top_shits[:to_check]] / close_df.loc[:, top_shits[:to_check]].max()).plot()
        plt.title(f'normalised {var_to_check}')
        plt.show()
        ret_df.loc[:, top_shits[:to_check]].plot()
        plt.title(f'daily log return of {var_to_check}')
        # plt.legend([])
        plt.show()

        if export_shits:
            secd_all_df = pd.read_csv(all_dir + 'comp_secd_all.csv', low_memory=False)
            for shitty_ric in top_shits[:to_check]:
                shitty_gvkey = us.ric2num(shitty_ric)
                shitty_df = secd_all_df[secd_all_df['gvkey'] == shitty_gvkey]
                shitty_df = shitty_df.set_index(us.date_col_finder(shitty_df, shitty_ric))
                shitty_df = shitty_df.sort_index()
                shitty_df.to_csv(f'../data/validity_check/{shitty_ric}.csv')
    us.beep()

mom_dist_plot = False
if mom_dist_plot:
    close_df = pd.read_csv(secd_dir + '/close.csv')
    close_df = close_df.set_index(us.date_col_finder(close_df, 'close'))
    close_df = us.fillna(close_df, hat_in_cols=True)
    return_df = close_df / close_df.shift(1)
    return_df = return_df.astype(float).apply(np.log)
    return_df = return_df.replace([float('inf'), float('-inf'), 0], float('NaN'))

    plots.distribution_plot3d(return_df, 'datadate', 'firms', 'returns',
                              x_axis='log return', y_axis='', z_axis='portion of firms',
                              title='Distribution of log return over time',
                              filename='../data/_presentation/return_dist.png')
