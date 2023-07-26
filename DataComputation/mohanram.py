import pandas as pd
from useful_fn import series_to_df, lag

from_ref_dir = '../files/by_data/from_ref/'
intermed_dir = '../files/by_data/intermed/'

at = pd.read_csv(from_ref_dir+'at.csv')
ni = pd.read_csv(from_ref_dir+'ni.csv')
ib = pd.read_csv(from_ref_dir+'ib.csv')
dp = pd.read_csv(from_ref_dir+'dp.csv')
oancf = pd.read_csv(from_ref_dir+'oancf.csv')
xrd = pd.read_csv(from_ref_dir+'xrd.csv')
capx = pd.read_csv(from_ref_dir+'capx.csv')
xad = pd.read_csv(from_ref_dir+'xad.csv')
roavol = pd.read_csv(intermed_dir+'roavol.csv')
sgrvol = pd.read_csv(intermed_dir+'sgrvol.csv')

atlagat = 2 / (at + lag(at))
roa = atlagat * ni
cfroa = atlagat * (oancf + (oancf == 0) * (ib + dp))
xrdint = atlagat * xrd
capxint = atlagat * capx
xadint = atlagat * xad
ms = (roa > series_to_df(roa.median(axis=1), roa.columns)) * 1 + \
     (cfroa > series_to_df(cfroa.median(axis=1), cfroa.columns)) * 1 + \
     (xrdint > series_to_df(xrdint.median(axis=1), xrdint.columns)) * 1 + \
     (capxint > series_to_df(capxint.median(axis=1), capxint.columns)) * 1 + \
     (xadint > series_to_df(xadint.median(axis=1), xadint.columns)) * 1 + \
     (oancf > ni) * 1 + \
     (roavol < series_to_df(roavol.median(axis=1), roavol.columns)) * 1 + \
     (sgrvol < series_to_df(sgrvol.median(axis=1), sgrvol.columns)) * 1
