import os

wrds = True

all_dir = '/'.join(os.getcwd().split('/')[:3]) + '/Desktop/wrds_data/all/'
useful_dir = '/'.join(os.getcwd().split('/')[:3]) + '/Desktop/wrds_data/useful/'

preprocessed_dir = '../data/preprocessed_wrds/' if wrds else '../data/preprocessed/'
processed_dir = '../data/processed_wrds/' if wrds else '../data/processed/'

funda_dir = preprocessed_dir + 'FY/'
fundq_dir = preprocessed_dir + 'FQ/'
secd_dir = preprocessed_dir + 'price/'
raw_dir = preprocessed_dir + '_raw/'

intermed_dir = processed_dir + 'intermed/'
by_var_dd_dir = processed_dir + 'output_by_var_dd/'
by_var_mm_dir = processed_dir + 'output_by_var_mm/'
output_dir = processed_dir + 'output/'
