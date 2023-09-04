import pandas as pd
import Misc.useful_stuff as us
import Misc.elementwise_calc as ec
import matplotlib.pyplot as plt
import _options as opt

wrds = opt.wrds

funda_dir = opt.funda_dir
fundq_dir = opt.fundq_dir
secd_dir = opt.secd_dir
intermed_dir = opt.intermed_dir

if __name__ == '__main__':
    close_ref = pd.read_csv('../data/processed/input_secd/close_adj.csv')
    close_wrds = pd.read_csv('../data/processed_wrds/input_secd/close.csv')

    close_ref[us.date_col_finder(close_ref, 'close_adj')] = us.dt_to_str(close_ref[us.date_col_finder(close_ref, 'close_adj')])
    close_wrds[us.date_col_finder(close_wrds, 'close')] = us.dt_to_str(close_wrds[us.date_col_finder(close_wrds, 'close')])
    close_ref = close_ref.set_index(us.date_col_finder(close_ref, 'close_adj'))
    close_wrds = close_wrds.set_index(us.date_col_finder(close_wrds, 'close'))

    # ric-ified gvkeys -> gvkeys -> ric
    close_wrds.columns = pd.Series(close_wrds.columns).apply(lambda x: us.ric2num(x))
    ricdict = pd.read_csv('../data/metadata/ref-comp.csv')[['ric', 'gvkey']].set_index('gvkey').to_dict()
    close_wrds = close_wrds.rename(columns=ricdict['ric'])

    close_ref = close_ref.loc[close_ref.index[close_ref.index.isin(close_wrds.index)], close_ref.columns[close_ref.columns.isin(close_wrds.columns)]]
    close_wrds = close_wrds.loc[close_wrds.index[close_wrds.index.isin(close_ref.index)], close_wrds.columns[close_wrds.columns.isin(close_ref.columns)]]

    close_ref = close_ref.reindex(close_wrds.index, axis=0).reindex(close_wrds.columns, axis=1)
    count = 0
    loc1, loc2 = 0, 0
    fig, axs = plt.subplots(2, 2)
    for i, firm in enumerate(close_ref.columns):
        ser_from_ref = close_ref.loc[:, firm]
        ser_from_wrds = close_wrds.loc[:, firm]
        ser_from_ref = ser_from_ref / ser_from_ref.max()
        ser_from_wrds = ser_from_wrds / ser_from_wrds.max()
        df = pd.concat([ser_from_ref, ser_from_wrds], axis=1)
        df.plot(ax=axs[loc1, loc2], legend=None, xlabel=None)
        # plt.legend(['ref', 'wrds'])
        axs[loc1, loc2].set_xticks([])
        axs[loc1, loc2].set_yticks([])
        axs[loc1, loc2].set_xlabel(None)
        # axs[loc1, loc2].legend(['ref', 'wrds'])
        # axs[loc1, loc2].set_title(firm)

        loc2 = (loc2 + 1) % 2
        if loc2 == 0:
            loc1 = (loc1 + 1) % 2

        count += 1
        if count > 50:
            break

        if loc1 == 0 and loc2 == 0 and fig is not None:
            fig.supxlabel('Date')
            fig.supylabel('Normalized price')
            fig.legend(['ref', 'wrds'])
            fig.show()
            fig, axs = plt.subplots(2, 2)
