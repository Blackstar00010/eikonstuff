import pandas as pd
import Misc.useful_stuff as us
from Misc.elementwise_calc import lag, delta, past_stddev, past_mean

wrds = False

if __name__ == '__main__':
    fundq_dir = '../data/processed_wrds/input_fundq/' if wrds else '../data/processed/input_fundq/'
    secd_dir = '../data/processed_wrds/input_secd/' if wrds else '../data/processed/input_secd/'
    by_var_dir = '../data/processed_wrds/output_by_var_dd/' if wrds else '../data/processed/output_by_var_dd/'
    intermed_dir = '../data/processed_wrds/intermed/' if wrds else '../data/processed/intermed/'

    if True:
        pstkq = pd.read_csv(fundq_dir + 'pstkq.csv').set_index('datadate').fillna(0)
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
    sacc = (delta(actq) - delta(cheq) - delta(lctq) + delta(dlcq)) / abs_revtq
    stdacc = past_stddev(sacc, 16)
    sgrvol = past_stddev(rsup, 16)
    roavol = past_stddev(roaq, 16)
    scf = (ibq / abs_revtq) - sacc
    stdcf = past_stddev(scf, 16)
    cash = cheq / atq
    cinvest = past_mean(delta(ppentq) / abs_revtq, 4)
    che = delta(ibq, 4)

    ibq_incr = (ibq > lag(ibq)) * 1
    nincr = ibq_incr + ibq_incr * lag(ibq_incr) + ibq_incr * lag(ibq_incr) * lag(ibq_incr, 2) + \
            ibq_incr * lag(ibq_incr) * lag(ibq_incr, 2) * lag(ibq_incr, 2) + \
            ibq_incr * lag(ibq_incr) * lag(ibq_incr, 2) * lag(ibq_incr, 3)
    ibq_incr4 = ibq_incr * lag(ibq_incr) * lag(ibq_incr, 2) * lag(ibq_incr, 3) * lag(ibq_incr, 4)
    nincr += ibq_incr4 + ibq_incr4 * lag(ibq_incr, 5) + ibq_incr4 * lag(ibq_incr, 5) * lag(ibq_incr, 6) + \
             ibq_incr4 * lag(ibq_incr, 5) * lag(ibq_incr, 6) * lag(ibq_incr, 7)
    ibq_incr8 = ibq_incr4 * lag(ibq_incr, 5) * lag(ibq_incr, 6) * lag(ibq_incr, 7) * lag(ibq_incr, 8)
    nincr += ibq_incr8 + ibq_incr8 * lag(ibq_incr, 9) + ibq_incr8 * lag(ibq_incr, 9) * lag(ibq_incr, 10) + \
             ibq_incr8 * lag(ibq_incr, 9) * lag(ibq_incr, 10) * lag(ibq_incr, 11) + \
             ibq_incr8 * lag(ibq_incr, 9) * lag(ibq_incr, 10) * lag(ibq_incr, 11) * lag(ibq_incr, 12)

    roaq = roaq * (countq > 1)
    roeq = roeq * (countq > 1)
    chtx = chtx * (countq > 4)
    che = che * (countq > 4)
    cinvest = cinvest * (countq > 4)
    stdacc = stdacc * (countq > 16)
    stdcf = stdcf * (countq > 16)
    sgrvol = sgrvol * (countq > 16)
    roavol = roavol * (countq > 16)

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
