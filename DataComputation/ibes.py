import pandas as pd
import logging
from Misc.elementwise_calc import lag, delta, rate_of_change, ind_adj
from os.path import join as pathjoin

'''
Lines 642 - 700
'''

funda_dir = '../data/processed/input_funda/'
secd_dir = '../data/processed/input_secd/'
seventyeight_dir = '../data/processed/output_by_var_dd/'
intermed_dir = '../data/processed/intermed/'

if __name__ == '__main__':
    print('ibes.py is being run directly')
    if True:
        medest = pd.DataFrame()
        actual = pd.DataFrame()

        che = pd.read_csv(pathjoin(funda_dir, 'che.csv')).set_index('datadate').fillna(0)
        mve = pd.read_csv(pathjoin(secd_dir, 'mve.csv')).set_index('Date').fillna(0)
        prc = pd.read_csv(pathjoin(secd_dir, 'close.csv')).set_index('Date').fillna(0)

    sue = (medest!=0) * (actual!=0) * (actual-medest) / abs(prc) + ((medest==0) + (actual==0)) * che/mve
