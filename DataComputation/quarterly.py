import pandas as pd
import Misc.useful_stuff as us
from Misc.elementwise_calc import lag, delta, past_stddev, past_mean
import _options as opt


wrds = opt.wrds

funda_dir = opt.funda_dir
fundq_dir = opt.fundq_dir
secd_dir = opt.secd_dir
intermed_dir = opt.intermed_dir
by_var_dir = opt.by_var_dd_dir

if __name__ == '__main__':
    if True:
        try:
            pstkq = pd.read_csv(fundq_dir + 'pstkq.csv').set_index('datadate').fillna(0)
        except FileNotFoundError:
            pstkq = pd.read_csv(funda_dir + 'pstk.csv').set_index('datadate').fillna(0) / 4
        try:
            pstkrq = pd.read_csv(fundq_dir + 'pstkrq.csv').set_index('datadate').fillna(0)
        except FileNotFoundError:
            pstkrq = pd.DataFrame(0, columns=pstkq.columns, index=pstkq.index)
        ceqq = pd.read_csv(fundq_dir + 'ceqq.csv').set_index('datadate').fillna(0)
        try:
            seqq = pd.read_csv(fundq_dir + 'seqq.csv').set_index('datadate').fillna(0)
        except FileNotFoundError:
            seqq = pd.DataFrame(float('NaN'), columns=ceqq.columns, index=ceqq.index)
        atq = pd.read_csv(fundq_dir + 'atq.csv').set_index('datadate').fillna(0)
        ltq = pd.read_csv(fundq_dir + 'ltq.csv').set_index('datadate').fillna(0)
        txtq = pd.read_csv(fundq_dir + 'txtq.csv').set_index('datadate').fillna(0)
        ibq = pd.read_csv(fundq_dir + 'ibq.csv').set_index('datadate').fillna(0)
        revtq = pd.read_csv(fundq_dir + 'revtq.csv').set_index('datadate').fillna(0)
        actq = pd.read_csv(fundq_dir + 'actq.csv').set_index('datadate').fillna(0)
        cheq = pd.read_csv(fundq_dir + 'cheq.csv').set_index('datadate').fillna(0)
        lctq = pd.read_csv(fundq_dir + 'lctq.csv').set_index('datadate').fillna(0)
        dlcq = pd.read_csv(fundq_dir + 'dlcq.csv').set_index('datadate').fillna(0)
        ppentq = pd.read_csv(fundq_dir + 'ppentq.csv').set_index('datadate').fillna(0)
        countq = pd.read_csv(fundq_dir + 'countq.csv').set_index('datadate').fillna(0)

        mveq = pd.read_csv(secd_dir + 'mve.csv')
        mveq = mveq.set_index(us.date_col_finder(mveq, 'mveq')).fillna(0)
        print('all data loaded.')

    # lines 559-
    pstk = pstkq.fillna(pstkrq)
    scal = seqq.fillna(ceqq + pstk).fillna(atq - ltq)
    chtx = delta(txtq, by=4) / lag(atq, by=4)
    roaq = ibq / lag(atq)
    roeq = ibq / lag(scal)
    rsup = delta(revtq, by=4) / mveq

    abs_revtq = (revtq * (revtq > 0) + 0.01 * (revtq <= 0))
    sacc = delta(actq-cheq-lctq+dlcq) / abs_revtq
    stdacc = past_stddev(sacc, 16)
    sgrvol = past_stddev(rsup, 16)
    print('ten computed!')
    roavol = past_stddev(roaq, 16)
    scf = (ibq / abs_revtq) - sacc
    stdcf = past_stddev(scf, 16)
    cash = cheq / atq
    cinvest = past_mean(delta(ppentq) / abs_revtq, 4)
    che = delta(ibq, 4)

    ibq_incr = (ibq > lag(ibq)) * 1
    nincr = ibq_incr
    for i in range(1, 4 * 4):  # b/c we are using 48 months
        nincr += ibq_incr * lag(ibq_incr, i) * 1

    roaq = roaq * (countq > 1)
    roeq = roeq * (countq > 1)
    print('ten computed!')
    chtx = chtx * (countq > 4)
    che = che * (countq > 4)
    cinvest = cinvest * (countq > 4)
    stdacc = stdacc * (countq > 16)
    stdcf = stdcf * (countq > 16)
    sgrvol = sgrvol * (countq > 16)
    roavol = roavol * (countq > 16)

    print('Saving...')
    us.fix_save_df(chtx, by_var_dir, 'chtx.csv', index_label='datadate')
    us.fix_save_df(cash, by_var_dir, 'cash.csv', index_label='datadate')
    us.fix_save_df(nincr, by_var_dir, 'nincr.csv', index_label='datadate')
    us.fix_save_df(roaq, by_var_dir, 'roaq.csv', index_label='datadate')
    us.fix_save_df(roeq, by_var_dir, 'roeq.csv', index_label='datadate')
    us.fix_save_df(chtx, by_var_dir, 'chtx.csv', index_label='datadate')
    us.fix_save_df(che, by_var_dir, 'che.csv', index_label='datadate')
    us.fix_save_df(cinvest, by_var_dir, 'cinvest.csv', index_label='datadate')
    us.fix_save_df(stdacc, by_var_dir, 'stdacc.csv', index_label='datadate')
    us.fix_save_df(stdcf, by_var_dir, 'stdcf.csv', index_label='datadate')
    us.fix_save_df(sgrvol, by_var_dir, 'sgrvol.csv', index_label='datadate')
    us.fix_save_df(roavol, by_var_dir, 'roavol.csv', index_label='datadate')

    us.beep()
