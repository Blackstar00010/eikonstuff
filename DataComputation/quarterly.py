import pandas as pd
from Misc.elementwise_calc import lag, delta, past_stddev, past_mean

if __name__ == '__main__':
    from_ref_dir = '../data/processed/input_funda/'
    seventyeight_dir = '../data/processed/output_by_var_dd/'
    intermed_dir = '../data/processed/intermed/'

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

    # lines 559-
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
    chtx = chtx * (countq > 4)
    che = che * (countq > 4)
    cinvest = cinvest * (countq > 4)
    stdacc = stdacc * (countq > 16)
    stdcf = stdcf * (countq > 16)
    sgrvol = sgrvol * (countq > 16)
    roavol = roavol * (countq > 16)

    # pstk.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(seventyeight_dir + 'pstk.csv')
    # scal.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(seventyeight_dir + 'scal.csv')
    rsup.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(seventyeight_dir + 'rsup.csv')
    # abs_revtq.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(seventyeight_dir + 'abs_revtq.csv')
    # sacc.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(seventyeight_dir + 'sacc.csv')
    # scf.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(seventyeight_dir + 'scf.csv')
    cash.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(seventyeight_dir + 'cash.csv')
    # ibq_incr.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(seventyeight_dir + 'ibq_incr.csv')
    # ibq_incr4.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(seventyeight_dir + 'ibq_incr4.csv')
    # ibq_incr8.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(seventyeight_dir + 'ibq_incr8.csv')
    nincr.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(seventyeight_dir + 'nincr.csv')
    roaq.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(seventyeight_dir + 'roaq.csv')
    roeq.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(seventyeight_dir + 'roeq.csv')
    chtx.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(seventyeight_dir + 'chtx.csv')
    che.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(intermed_dir + 'che.csv')
    cinvest.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(seventyeight_dir + 'cinvest.csv')
    stdacc.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(seventyeight_dir + 'stdacc.csv')
    stdcf.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(seventyeight_dir + 'stdcf.csv')
    sgrvol.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(intermed_dir + 'sgrvol.csv')
    roavol.replace([0, float('inf'), -float('inf')], float('NaN')).to_csv(seventyeight_dir + 'roavol.csv')