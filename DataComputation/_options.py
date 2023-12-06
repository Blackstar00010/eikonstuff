import os

wrds = True

all_dir = '/'.join(os.getcwd().split('/')[:3]) + '/Desktop/wrds_data/all/'
useful_dir = '/'.join(os.getcwd().split('/')[:3]) + '/Desktop/wrds_data/useful/'

preprocessed_dir = '../data/preprocessed_wrds/' if wrds else '../data/preprocessed/'
processed_dir = '../data/processed_wrds/' if wrds else '../data/processed/'

raw_dir = preprocessed_dir + '_raw/'
funda_dir = processed_dir + 'input_funda/'
fundq_dir = processed_dir + 'input_fundq/'
secd_dir = processed_dir + 'input_secd/'

intermed_dir = processed_dir + 'intermed/'
by_var_dd_dir = processed_dir + 'output_by_var_dd/'
by_var_mm_dir = processed_dir + 'output_by_var_mm/'
output_dir = processed_dir + 'output/'
percentile_output_dir = processed_dir + 'output_percentile/'
