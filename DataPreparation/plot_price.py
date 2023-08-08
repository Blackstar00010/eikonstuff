import pandas as pd
import matplotlib.pyplot as plt
from Misc import useful_stuff as us

the_dir = '../data/price_stuff/index_data/'
files = us.listdir(the_dir)

normaliseQ = ''
for afile in files:
    df = pd.read_csv(the_dir + afile)
    df = df.set_index(us.date_col_finder(df, afile))

    for to_unplot in ['VOLUME', 'volume', 'Volume', 'COUNT', 'count', 'Count']:
        if to_unplot in df.columns:
            df = df.drop(to_unplot, axis=1)

    if type(normaliseQ) is not bool:
        normaliseQ = input('Do you wish to normalise? [y/n] ')
        while normaliseQ not in ['y', 'n', 'Y', 'N', 'ㅛ', 'ㅜ']:
            normaliseQ = input('Do you wish to normalise? [y/n] ')
        normaliseQ = True if normaliseQ in ['y', 'Y', 'ㅛ'] else False
    if normaliseQ:
        for acol in df.columns:
            div_factor = df.loc[df[acol].first_valid_index(), acol]
            df.loc[:, acol] /= div_factor
    df.plot()
    plt.title(afile)
    plt.yscale('log')
    plt.show()
