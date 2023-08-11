import pandas as pd
import numpy as np
from Misc.elementwise_calc import series_to_df

from_ref_dir = '../data/processed/input_funda/'
intermed_dir = '../data/processed/intermed/'

ni = pd.read_csv(from_ref_dir+'ni.csv').set_index('datadate')
ib = pd.read_csv(from_ref_dir+'ib.csv').set_index('datadate')
dp = pd.read_csv(from_ref_dir+'dp.csv').set_index('datadate')
oancf = pd.read_csv(from_ref_dir+'oancf.csv').set_index('datadate')
xrd = pd.read_csv(from_ref_dir+'xrd.csv').set_index('datadate')
capx = pd.read_csv(from_ref_dir+'capx.csv').set_index('datadate')
xad = pd.read_csv(from_ref_dir+'xad.csv').set_index('datadate')
sic2 = pd.DataFrame()  # todo

roavol = pd.read_csv(intermed_dir+'roavol.csv').set_index('datadate')
sgrvol = pd.read_csv(intermed_dir+'sgrvol.csv').set_index('datadate')
atlagat = pd.read_csv(intermed_dir+'atlagat.csv').set_index('datadate')

roa = atlagat * ni
cfroa = atlagat * (oancf + (oancf == 0) * (ib + dp))
xrdint = atlagat * xrd
capxint = atlagat * capx
xadint = atlagat * xad

# todo: ms last two lines -> these lines below
# sic2list = pd.DataFrame()
# for acol in sic2.columns:
#     sic2list = np.append(sic2list, sic2[acol].unique())
#     sic2list = np.unique(sic2list)
#
# roavol_score = pd.DataFrame()
# for asic2 in sic2list:
#     comp_group = sic2==asic2
#     roavol_group = roavol[comp_group].replace([0, float('inf'), -float('inf')], float('NaN'))
#     roavol_med = roavol_group.median(axis=1)
#     roavol_med = series_to_df(roavol_med, roavol_group.columns)
#     roavol_score = roavol_score.append((roavol_group > roavol_med) * 1)

ms = (roa > series_to_df(roa.median(axis=1), roa.columns)) * 1 + \
     (cfroa > series_to_df(cfroa.median(axis=1), cfroa.columns)) * 1 + \
     (xrdint > series_to_df(xrdint.median(axis=1), xrdint.columns)) * 1 + \
     (capxint > series_to_df(capxint.median(axis=1), capxint.columns)) * 1 + \
     (xadint > series_to_df(xadint.median(axis=1), xadint.columns)) * 1 + \
     (oancf > ni) * 1 + \
     (roavol < series_to_df(roavol.median(axis=1), roavol.columns)) * 1 + \
     (sgrvol < series_to_df(sgrvol.median(axis=1), sgrvol.columns)) * 1
