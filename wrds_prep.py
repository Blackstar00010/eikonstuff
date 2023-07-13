import pandas as pd

wrds_dir = './files/wrds/'
"""
ibes_qtr_est.csv
comp_funda.csv
comp_fundq.csv
ibes_ann_est.csv
comp_adsprate.csv
comp_company.csv
comp_exrt.csv
"""

df = pd.read_csv(wrds_dir+'ibes_qtr_est.csv')
print(df)
