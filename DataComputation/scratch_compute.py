import pandas as pd
import Misc.useful_stuff as us
import Misc.elementwise_calc as ec
import matplotlib.pyplot as plt

wrds = True

funda_dir = '../data/processed_wrds/input_funda/' if wrds else '../data/processed/input_funda/'
fundq_dir = '../data/processed_wrds/input_fundq/' if wrds else '../data/processed/input_fundq/'
secd_dir = '../data/processed_wrds/input_secd/' if wrds else '../data/processed/input_secd/'
intermed_dir = '../data/processed_wrds/intermed/' if wrds else '../data/processed/intermed/'

if __name__ == '__main__':
    close_ref = pd.read_csv('../data/processed/input_secd/close_adj.csv')
    close_wrds = pd.read_csv('../data/processed_wrds/input_secd/close.csv')

    close_ref[us.date_col_finder(close_ref, 'close_adj')] = us.dt_to_str(close_ref[us.date_col_finder(close_ref, 'close_adj')])
    close_wrds[us.date_col_finder(close_wrds, 'close')] = us.dt_to_str(close_wrds[us.date_col_finder(close_wrds, 'close')])
    close_ref = close_ref.set_index(us.date_col_finder(close_ref, 'close_adj'))
    close_wrds = close_wrds.set_index(us.date_col_finder(close_wrds, 'close'))

    close_ref = close_ref.loc[close_ref.index[close_ref.index.isin(close_wrds.index)], close_ref.columns[close_ref.columns.isin(close_wrds.columns)]]
    close_wrds = close_wrds.loc[close_wrds.index[close_wrds.index.isin(close_ref.index)], close_wrds.columns[close_wrds.columns.isin(close_ref.columns)]]

    close_ref = close_ref.reindex(close_wrds.index, axis=0).reindex(close_wrds.columns, axis=1)
    count = 0
    for i, firm in enumerate(close_ref.columns):
        ser_from_ref = close_ref.loc[:, firm]
        ser_from_wrds = close_wrds.loc[:, firm]
        ser_from_wrds = ser_from_wrds * ser_from_ref.max() / ser_from_wrds.max()
        df = pd.concat([ser_from_ref, ser_from_wrds], axis=1)
        df.plot()
        plt.legend(['ref', 'wrds'])
        plt.title(firm)
        plt.show()
