import pandas as pd
from Misc.elementwise_calc import series_to_df
import _options as opt
import Misc.useful_stuff as us

wrds = opt.wrds

funda_dir = opt.funda_dir
fundq_dir = opt.fundq_dir
secd_dir = opt.secd_dir
intermed_dir = opt.intermed_dir
by_var_dir = opt.by_var_dd_dir


def run_mohanram(beep_on_finish=True):
    if __name__ != '__main__':
        print('Computing Mohanram score...')
    ib = pd.read_csv(funda_dir + 'ib.csv').set_index('datadate').replace(float('NaN'), 0)
    try:
        ni = pd.read_csv(funda_dir + 'ni.csv').set_index('datadate').replace(float('NaN'), 0)
    except FileNotFoundError:
        ni = ib - pd.read_csv(funda_dir + 'txt.csv').set_index('datadate').replace(float('NaN'), 0)
    dp = pd.read_csv(funda_dir + 'dp.csv').set_index('datadate').replace(float('NaN'), 0)
    xrd = pd.read_csv(funda_dir + 'xrd.csv').set_index('datadate').replace(float('NaN'), 0)
    capx = pd.read_csv(funda_dir + 'capx.csv').set_index('datadate').replace(float('NaN'), 0)
    try:
        xad = pd.read_csv(funda_dir + 'xad.csv').set_index('datadate').replace(float('NaN'), 0)
    except:
        xad = pd.DataFrame(0, columns=capx.columns, index=capx.index)
    sic2 = pd.DataFrame()  # todo

    oancf = pd.read_csv(intermed_dir + 'oancf_alt.csv').set_index('datadate').replace(float('NaN'), 0)
    atlagat = pd.read_csv(intermed_dir+'atlagat.csv').set_index('datadate').replace(float('NaN'), 0)
    roavol = pd.read_csv(by_var_dir+'roavol.csv').set_index('datadate').replace(float('NaN'), 0)
    sgrvol = pd.read_csv(by_var_dir+'sgrvol.csv').set_index('datadate').replace(float('NaN'), 0)

    print('all data loaded.')

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

    ni_filler = pd.DataFrame(0,
                             columns=oancf.columns[~oancf.columns.isin(ni.columns)],
                             index=oancf.index[~oancf.index.isin(ni.index)])
    oancf_filler = pd.DataFrame(0,
                                columns=ni.columns[~ni.columns.isin(oancf.columns)],
                                index=ni.index[~ni.index.isin(oancf.index)])
    ni = pd.concat([ni, ni_filler])
    ni = ni.sort_index(axis=1).sort_index(axis=0)
    oancf = pd.concat([oancf, oancf_filler])
    oancf = oancf.sort_index(axis=1).sort_index(axis=0)

    ms = (roa > series_to_df(roa.median(axis=1), roa.columns)) * 1 + \
         (cfroa > series_to_df(cfroa.median(axis=1), cfroa.columns)) * 1 + \
         (xrdint > series_to_df(xrdint.median(axis=1), xrdint.columns)) * 1 + \
         (capxint > series_to_df(capxint.median(axis=1), capxint.columns)) * 1 + \
         (xadint > series_to_df(xadint.median(axis=1), xadint.columns)) * 1 + \
         (oancf > ni) * 1 + \
         (roavol < series_to_df(roavol.median(axis=1), roavol.columns)) * 1 + \
         (sgrvol < series_to_df(sgrvol.median(axis=1), sgrvol.columns)) * 1

    ms.to_csv(by_var_dir + 'ms.csv', index=True)

    if beep_on_finish:
        us.beep()

if __name__ == '__main__':
    run_mohanram()
