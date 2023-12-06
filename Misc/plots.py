import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import Misc.useful_stuff as us


def distribution_plot3d(some_df: pd.DataFrame,
                        id_name: str = None, var_name: str = None, value_name: str = None,
                        x_axis: str = None, y_axis: str = None, z_axis: str = None,
                        filename: str = None, title: str = None, plot_range = None):
    """
    Plots the 3d distribution of a DataFrame.
    :param some_df: df that contains the data to plot.
        The distribution of the values in a row will be plotted.
        The values in the df should be numeric.
    :param id_name: name of the column that contains the id of each row. If None, it will be the name of the date column.
    :param var_name: name of the column that contains the variable name of each row. If None, it will be 'firms'.
    :param value_name: name of the column that contains the value of each row. If None, it will be 'returns'.
    :return:
    """
    if id_name is None:
        try:
            id_name = us.date_col_finder(some_df, 'df_to_plot_distribution')
        except IndexError:
            some_df = some_df.reset_index()
            id_name = us.date_col_finder(some_df, 'df_to_plot_distribution')
    else:
        if id_name not in some_df.columns:
            some_df = some_df.reset_index()
    if var_name is None:
        var_name = 'firms'
    if value_name is None:
        value_name = 'returns'

    melted_df = pd.melt(some_df, id_vars=[id_name], var_name=var_name, value_name=value_name)
    melted_df = melted_df.dropna(subset=[value_name])

    return_bins = np.linspace(melted_df[value_name].min(), melted_df[value_name].max(), 100)
    time_bins = melted_df[id_name].unique()

    melted_df['range'] = pd.cut(melted_df[value_name], bins=return_bins, labels=return_bins[:-1])
    distribution_df = melted_df.groupby([id_name, 'range']).size().unstack(fill_value=0)
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

    if plot_range in ['full', 'Full']:
        ax.xlim = (distribution_df.columns.min(), distribution_df.columns.max())
    elif plot_range is not None:
        ax.xlim = plot_range
    else:
        mean = distribution_df.mean(axis=0)
        std = distribution_df.std(axis=0)
        ax.xlim = (mean - 3 * std, mean + 3 * std)

    # Y ticks: Limited to 5 ticks
    num_y_ticks = 5
    y_tick_positions = np.linspace(0, distribution_df.shape[0] - 1, num_y_ticks, dtype=int)
    y_tick_labels = [x[:7] for x in time_bins[y_tick_positions]]
    ax.set_yticks(y_tick_positions)
    ax.set_yticklabels(y_tick_labels)

    plt.title(title)
    ax.set_xlabel(x_axis)
    ax.set_ylabel(y_axis)
    ax.set_zlabel(z_axis)

    if filename is not None:
        plt.savefig(filename, dpi=300)

    plt.show()
