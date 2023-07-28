import pandas as pd
from elementwise_calc import lag, delta, rate_of_change, past_stddev, past_mean

if __name__ == '__main__':
    from_ref_dir = '../files/by_data/from_ref/'

    if True:
        pstkrq = pd.read_csv(from_ref_dir + 'pstkrq.csv').set_index('datadate')
        pstkq = pd.read_csv(from_ref_dir + 'pstkq.csv').set_index('datadate')
        seqq = pd.read_csv(from_ref_dir + 'seqq.csv').set_index('datadate')
        ceqq = pd.read_csv(from_ref_dir + 'ceqq.csv').set_index('datadate')
        atq = pd.read_csv(from_ref_dir + 'atq.csv').set_index('datadate')
        ltq = pd.read_csv(from_ref_dir + 'ltq.csv').set_index('datadate')
        txtq = pd.read_csv(from_ref_dir + 'txtq.csv').set_index('datadate')
        ibq = pd.read_csv(from_ref_dir + 'ibq.csv').set_index('datadate')
        revtq = pd.read_csv(from_ref_dir + 'revtq.csv').set_index('datadate')
        actq = pd.read_csv(from_ref_dir + 'actq.csv').set_index('datadate')
        cheq = pd.read_csv(from_ref_dir + 'cheq.csv').set_index('datadate')
        lctq = pd.read_csv(from_ref_dir + 'lctq.csv').set_index('datadate')
        dlcq = pd.read_csv(from_ref_dir + 'dlcq.csv').set_index('datadate')
        ppentq = pd.read_csv(from_ref_dir + 'ppentq.csv').set_index('datadate')
        countq = pd.read_csv(from_ref_dir + 'countq.csv').set_index('datadate')
        mveq = pd.read_csv(from_ref_dir + 'mveq.csv').set_index('datadate')

    pstk = pstkrq.fillna(pstkq)
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
    chtx = chtx * (countq > 5)
    che = che * (countq > 5)
    cinvest = cinvest * (countq > 5)
    stdacc = stdacc * (countq > 17)
    stdcf = stdcf * (countq > 17)
    sgrvol = sgrvol * (countq > 17)
    roavol = roavol * (countq > 17)
