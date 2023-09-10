import os

wrds = True

all_dir = '/'.join(os.getcwd().split('/')[:3]) + '/Desktop/wrds_data/all/'
useful_dir = '/'.join(os.getcwd().split('/')[:3]) + '/Desktop/wrds_data/useful/'

funda_dir = '../data/processed_wrds/input_funda/' if wrds else '../data/processed/input_funda/'
fundq_dir = '../data/processed_wrds/input_fundq/' if wrds else '../data/processed/input_fundq/'
secd_dir = '../data/processed_wrds/input_secd/' if wrds else '../data/processed/input_secd/'
intermed_dir = '../data/processed_wrds/intermed/' if wrds else '../data/processed/intermed/'

by_var_dd_dir = '../data/processed_wrds/output_by_var_dd/' if wrds else '../data/processed/output_by_var_dd/'
by_var_mm_dir = '../data/processed_wrds/output_by_var_mm/' if wrds else '../data/processed/output_by_var_mm/'
output_dir = '../data/processed_wrds/output/' if wrds else '../data/processed/output/'
