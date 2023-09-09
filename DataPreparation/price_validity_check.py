import pandas as pd
import Misc.useful_stuff as us
import DataComputation._options as opt
import matplotlib.pyplot as plt
import numpy as np

secd_dir = opt.secd_dir
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
if mom_anomaly_check:
    close_df = pd.read_csv('../data/preprocessed_wrds/price/close.csv')
    close_df = close_df.set_index(us.date_col_finder(close_df, 'close'))
    # close_df = us.fillna(close_df, 'ffill')
    ret_df = close_df / close_df.shift(1) - 1
    shits = np.log(ret_df).replace([float('inf'), -float('inf'), 0], float('NaN')).dropna().abs().max().sort_values(
        ascending=False)
    top_shits = ret_df.max().sort_values(ascending=False).index[:1000]
    print(shits)
    # for i, shit in enumerate(top_shits):
    #     print(i, shit)

    close_df.loc[:, top_shits[:10]].plot()
    plt.title('Close')
    plt.show()
    (close_df.loc[:, top_shits[:10]] / close_df.loc[:, top_shits[:10]].max()).plot()
    plt.title('Normalised Close')
    plt.show()
    ret_df.loc[:, top_shits[:10]].plot()
    plt.title('Return')
    plt.show()

mom_dist_plot = False
if mom_dist_plot:
    close_df = pd.read_csv(secd_dir + '/close.csv')
    close_df = close_df.set_index(us.date_col_finder(close_df, 'close'))
    close_df = us.fillna(close_df, hat_in_cols=True)
    return_df = close_df / close_df.shift(1)
    return_df = return_df.astype(float).apply(np.log)
    return_df = return_df.replace([float('inf'), float('-inf'), 0], float('NaN'))

    melted_df = pd.melt(return_df.reset_index(), id_vars=['datadate'], var_name='firms', value_name='returns')
    melted_df = melted_df.dropna(subset=['returns'])

    return_bins = np.linspace(melted_df['returns'].min(), melted_df['returns'].max(), 100)
    time_bins = melted_df['datadate'].unique()

    melted_df['range'] = pd.cut(melted_df['returns'], bins=return_bins, labels=return_bins[:-1])
    distribution_df = melted_df.groupby(['datadate', 'range']).size().unstack(fill_value=0)
    distribution_df = distribution_df / distribution_df.sum(axis=1).values.reshape(-1, 1)

    X, Y = np.meshgrid(np.arange(distribution_df.shape[1]), np.arange(distribution_df.shape[0]))
    Z = distribution_df.values

    # Create a figure and a 3D subplot
    fig = plt.figure()
    ax = fig.add_subplot(111, projection='3d')
    ax.plot_surface(X, Y, Z, cmap='viridis')
    ax.set_title('Distribution of log return over time')

    # X ticks: Limited to 5 ticks
    num_x_ticks = 5
    x_tick_positions = np.linspace(0, distribution_df.shape[1] - 1, num_x_ticks, dtype=int)
    x_tick_labels = return_bins[x_tick_positions].round(2)
    ax.set_xticks(x_tick_positions)
    ax.set_xticklabels(x_tick_labels)

    # Y ticks: Limited to 5 ticks
    num_y_ticks = 5
    y_tick_positions = np.linspace(0, distribution_df.shape[0] - 1, num_y_ticks, dtype=int)
    y_tick_labels = [x[:7] for x in time_bins[y_tick_positions]]
    ax.set_yticks(y_tick_positions)
    ax.set_yticklabels(y_tick_labels)

    plt.savefig('../data/_presentation/return_dist.png', dpi=300)

    plt.show()
