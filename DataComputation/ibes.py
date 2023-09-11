import pandas as pd
import logging
from Misc.elementwise_calc import lag, delta, rate_of_change, ind_adj
from os.path import join as pathjoin
import _options as opt
import Misc.useful_stuff as us

'''
Lines 642 - 700
'''

wrds = opt.wrds

funda_dir = opt.funda_dir
fundq_dir = opt.fundq_dir
secd_dir = opt.secd_dir
intermed_dir = opt.intermed_dir
by_var_dir = opt.by_var_dd_dir


def run_ibes(beep_on_finish=True):
    if __name__ != '__main__':
        print('Computing firm characteristics from IBES data...')
    if True:
        medest = pd.DataFrame()
        actual = pd.DataFrame()

        che = pd.read_csv(pathjoin(funda_dir, 'che.csv')).set_index('datadate').fillna(0)
        mve = pd.read_csv(pathjoin(secd_dir, 'mve.csv')).set_index('Date').fillna(0)
        prc = pd.read_csv(pathjoin(secd_dir, 'close.csv')).set_index('Date').fillna(0)

    sue = (medest != 0) * (actual != 0) * (actual - medest) / abs(prc) + ((medest == 0) + (actual == 0)) * che / mve

    if beep_on_finish:
        us.beep()


if __name__ == '__main__':
    run_ibes()
