import pandas as pd
import Misc.useful_stuff as us
import Misc.elementwise_calc as ec

wrds = True

funda_dir = '../data/processed_wrds/input_funda/' if wrds else '../data/processed/input_funda/'
fundq_dir = '../data/processed_wrds/input_fundq/' if wrds else '../data/processed/input_fundq/'
secd_dir = '../data/processed_wrds/input_secd/' if wrds else '../data/processed/input_secd/'
intermed_dir = '../data/processed_wrds/intermed/' if wrds else '../data/processed/intermed/'

if __name__ == '__main__':
    xrd = pd.read_csv(funda_dir + 'xrd.csv').set_index('datadate').fillna(0)
    at = pd.read_csv(funda_dir + 'at.csv').set_index('datadate').fillna(0)
    rd = (ec.rate_of_change(xrd / at) > .05) * 1
    ec.ind_adj(at)
    print((xrd != 0).sum().sum() / xrd.notna().sum().sum())
    print((at != 0).sum().sum() / at.notna().sum().sum())
    print((rd != 0).sum().sum() / rd.notna().sum().sum())
