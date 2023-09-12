import pandas as pd
import Misc.useful_stuff as us


def fix_secd(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fix the following: QHFN, NOAD, OFUO, SLKI, NIGY, PQRC, TUCE, TRLK, SNBN, PRKD, TNRS, PRJJ, PRJS, MOLR, PLWM, NJJO
    :param df: secd df
    :return: fixed secd df
    """
    close_col = 'close' if 'close' in df.columns else 'prccd'
    price_cols = ['open', 'high', 'low', 'close'] if close_col == 'close' else ['prcod', 'prchd', 'prcld', 'prccd']
    shrout_col = 'shrout' if close_col == 'close' else 'cshoc'
    vol_col = 'vol' if close_col == 'close' else 'cshtrd'
    other_cols = [shrout_col, vol_col]
    adj_col = 'adj' if close_col == 'close' else 'ajexdi'

    date_col = us.date_col_finder(df, df_name='secd')
    if 'gvkey' not in df.columns:
        raise ValueError('gvkey not in df.columns')

    qhfn_df = df[df['gvkey'] == us.ric2num('QHFN')]
    df = df[df['gvkey'] != us.ric2num('QHFN')]
    qhfn_df0 = qhfn_df[qhfn_df[date_col] <= '2010-05-09']
    qhfn_df = qhfn_df[~qhfn_df.index.isin(qhfn_df0.index)]
    qhfn_df1 = qhfn_df[qhfn_df[date_col] <= '2018-11-20']
    qhfn_df2 = qhfn_df[~qhfn_df.index.isin(qhfn_df1.index)]
    qhfn_df0.loc[:, price_cols] = qhfn_df0[price_cols] / qhfn_df0[close_col].iloc[-1] * qhfn_df1[close_col].iloc[0]
    qhfn_df2.loc[:, price_cols] = qhfn_df2[price_cols] / qhfn_df2[close_col].iloc[0] * qhfn_df1[close_col].iloc[-1]
    qhfn_df2.loc[:, other_cols] = qhfn_df2[other_cols] / qhfn_df2[close_col].iloc[0] * qhfn_df1[close_col].iloc[-1]
    qhfn_df = pd.concat([qhfn_df0, qhfn_df1, qhfn_df2], axis=0)

    noad_df = df[df['gvkey'] == us.ric2num('NOAD')]
    df = df[df['gvkey'] != us.ric2num('NOAD')]
    noad_df0 = noad_df[noad_df[date_col] <= '2002-12-18']
    noad_df1 = noad_df[~noad_df.index.isin(noad_df0.index)]
    noad_df0.loc[:, adj_col] = noad_df0[adj_col] / 100
    noad_df = pd.concat([noad_df0, noad_df1], axis=0)

    ofuo_df = df[df['gvkey'] == us.ric2num('OFUO')]
    df = df[df['gvkey'] != us.ric2num('OFUO')]
    ofuo_df0 = ofuo_df[ofuo_df[date_col] <= '2017-12-27']
    ofuo_df1 = ofuo_df[~ofuo_df.index.isin(ofuo_df0.index)]
    ofuo_df1.loc[:, price_cols] = ofuo_df1[price_cols] / ofuo_df1[close_col].iloc[0] * ofuo_df0[close_col].iloc[-1]
    ofuo_df1.loc[:, vol_col] = ofuo_df1[vol_col] / ofuo_df1[close_col].iloc[0] * ofuo_df0[close_col].iloc[-1]
    ofuo_df = pd.concat([ofuo_df0, ofuo_df1], axis=0)

    slki_df = df[df['gvkey'] == us.ric2num('SLKI')]
    df = df[df['gvkey'] != us.ric2num('SLKI')]
    slki_df0 = slki_df[slki_df[date_col] <= '2017-01-05']
    slki_df1 = slki_df[~slki_df.index.isin(slki_df0.index)]
    slki_df1.loc[:, price_cols] = slki_df1[price_cols] / slki_df1[close_col].iloc[0] * slki_df0[close_col].iloc[-1]
    slki_df = pd.concat([slki_df0, slki_df1], axis=0)

    nigy_df = df[df['gvkey'] == us.ric2num('NIGY')]
    df = df[df['gvkey'] != us.ric2num('NIGY')]
    nigy_df0 = nigy_df[nigy_df[date_col] <= '2004-11-08']
    nigy_df = nigy_df[~nigy_df.index.isin(nigy_df0.index)]
    nigy_df1 = nigy_df[nigy_df[date_col] <= '2005-02-24']
    nigy_df2 = nigy_df[~nigy_df.index.isin(nigy_df1.index)]

    nigy_df0.loc[:, price_cols] = nigy_df0[price_cols] * nigy_df0[shrout_col].iloc[-1] / nigy_df1[shrout_col].iloc[0]
    nigy_df0.loc[:, shrout_col] = nigy_df0[shrout_col] / nigy_df0[shrout_col].iloc[-1] * nigy_df1[shrout_col].iloc[0]
    nigy_df2.loc[:, price_cols] = nigy_df2[price_cols] * nigy_df2[shrout_col].iloc[0] / nigy_df1[shrout_col].iloc[-1]
    nigy_df2.loc[:, shrout_col] = nigy_df2[shrout_col] / nigy_df2[shrout_col].iloc[0] * nigy_df1[shrout_col].iloc[-1]
    nigy_df = pd.concat([nigy_df0, nigy_df1], axis=0)

    pqrc_df = df[df['gvkey'] == us.ric2num('PQRC')]
    df = df[df['gvkey'] != us.ric2num('PQRC')]
    pqrc_df0 = pqrc_df[pqrc_df[date_col] <= '2012-01-26']
    pqrc_df1 = pqrc_df[~pqrc_df.index.isin(pqrc_df0.index)]
    pqrc_df0.loc[:, adj_col] = 0.0001
    pqrc_df = pd.concat([pqrc_df0, pqrc_df1], axis=0)

    tuce_df = df[df['gvkey'] == us.ric2num('TUCE')]
    df = df[df['gvkey'] != us.ric2num('TUCE')]
    tuce_df0 = tuce_df[tuce_df[date_col] <= '2021-04-23']
    tuce_df1 = tuce_df[~tuce_df.index.isin(tuce_df0.index)]
    tuce_df0.loc[:, shrout_col] = tuce_df1[shrout_col].iloc[0]
    tuce_df = pd.concat([tuce_df0, tuce_df1], axis=0)

    trlk_df = df[df['gvkey'] == us.ric2num('TRLK')]
    df = df[df['gvkey'] != us.ric2num('TRLK')]
    trlk_df0 = trlk_df[trlk_df[date_col] <= '2021-03-22']
    trlk_df1 = trlk_df[~trlk_df.index.isin(trlk_df0.index)]
    trlk_df0.loc[:, shrout_col] = trlk_df1[shrout_col].iloc[0]
    trlk_df = pd.concat([trlk_df0, trlk_df1], axis=0)

    snbn_df = df[df['gvkey'] == us.ric2num('SNBN')]
    df = df[df['gvkey'] != us.ric2num('SNBN')]
    snbn_df0 = snbn_df[snbn_df[date_col] <= '2018-09-27']
    snbn_df1 = snbn_df[~snbn_df.index.isin(snbn_df0.index)]
    snbn_df0.loc[:, price_cols] = snbn_df0[price_cols] / snbn_df0[close_col].iloc[-1] * snbn_df1[close_col].iloc[0]
    snbn_df = pd.concat([snbn_df0, snbn_df1], axis=0)

    prkd_df = df[df['gvkey'] == us.ric2num('PRKD')]
    df = df[df['gvkey'] != us.ric2num('PRKD')]
    prkd_df0 = prkd_df[prkd_df[date_col] <= '2006-03-31']
    prkd_df1 = prkd_df[~prkd_df.index.isin(prkd_df0.index)]
    prkd_df0.loc[:, price_cols] = prkd_df0[price_cols] / prkd_df0[close_col].iloc[-1] * prkd_df1[close_col].iloc[0]
    prkd_df0.loc[:, vol_col] = prkd_df0[vol_col] * prkd_df0[close_col].iloc[-1] / prkd_df1[close_col].iloc[0]
    prkd_df = pd.concat([prkd_df0, prkd_df1], axis=0)

    tnrs_df = df[df['gvkey'] == us.ric2num('TNRS')]
    df = df[df['gvkey'] != us.ric2num('TNRS')]
    tnrs_df0 = tnrs_df[tnrs_df[date_col] <= '2020-12-04']
    tnrs_df1 = tnrs_df[~tnrs_df.index.isin(tnrs_df0.index)]
    tnrs_df0.loc[:, shrout_col] = tnrs_df1[shrout_col].iloc[0] / tnrs_df0[adj_col].iloc[-1]
    tnrs_df1.loc[:, price_cols] = tnrs_df1[price_cols] / tnrs_df1[close_col].iloc[0] * (tnrs_df0[shrout_col].iloc[-1] * tnrs_df0[close_col].iloc[-1] / tnrs_df1[shrout_col].iloc[0])
    tnrs_df = pd.concat([tnrs_df0, tnrs_df1], axis=0)

    prjj_df = df[df['gvkey'] == us.ric2num('PRJJ')]
    df = df[df['gvkey'] != us.ric2num('PRJJ')]
    prjj_df0 = prjj_df[prjj_df[date_col] <= '2018-10-08']
    prjj_df = prjj_df[~prjj_df.index.isin(prjj_df0.index)]
    prjj_df1 = prjj_df[prjj_df[date_col] <= '2018-10-17']
    prjj_df2 = prjj_df[~prjj_df.index.isin(prjj_df1.index)]
    prjj_df1.loc[:, shrout_col] = prjj_df0[shrout_col].iloc[-1]
    prjj_df = pd.concat([prjj_df0, prjj_df1, prjj_df2], axis=0)

    prjs_df = df[df['gvkey'] == us.ric2num('PRJS')]
    df = df[df['gvkey'] != us.ric2num('PRJS')]
    prjs_df0 = prjs_df[prjs_df[date_col] <= '2018-06-12']
    prjs_df = prjs_df[~prjs_df.index.isin(prjs_df0.index)]
    prjs_df1 = prjs_df[prjs_df[date_col] <= '2018-06-18']
    prjs_df2 = prjs_df[~prjs_df.index.isin(prjs_df1.index)]
    prjs_df1.loc[:, shrout_col] = prjs_df2[shrout_col].iloc[-1]
    prjs_df = pd.concat([prjs_df0, prjs_df1, prjs_df2], axis=0)

    molr_df = df[df['gvkey'] == us.ric2num('MOLR')]
    df = df[df['gvkey'] != us.ric2num('MOLR')]
    molr_df0 = molr_df[molr_df[date_col] <= '2000-01-05']
    molr_df1 = molr_df[~molr_df.index.isin(molr_df0.index)]
    molr_df0.loc[:, shrout_col] = molr_df0[shrout_col] / molr_df0[shrout_col].iloc[-1] * molr_df1[shrout_col].iloc[0]
    molr_df = pd.concat([molr_df0, molr_df1], axis=0)

    plwm_df = df[df['gvkey'] == us.ric2num('PLWM')]
    df = df[df['gvkey'] != us.ric2num('PLWM')]
    plwm_df0 = plwm_df[plwm_df[date_col] <= '2009-10-13']
    plwm_df1 = plwm_df[~plwm_df.index.isin(plwm_df0.index)]
    plwm_df0.loc[:, shrout_col] = plwm_df0[shrout_col] / plwm_df0[shrout_col].iloc[-1] * plwm_df1[shrout_col].iloc[0]
    plwm_df = pd.concat([plwm_df0, plwm_df1], axis=0)

    njjo_df = df[df['gvkey'] == us.ric2num('NJJO')]
    df = df[df['gvkey'] != us.ric2num('NJJO')]
    njjr_df0 = njjo_df[njjo_df[date_col] <= '2020-05-01']
    njjr_df1 = njjo_df[~njjo_df.index.isin(njjr_df0.index)]
    njjr_df0.loc[:, price_cols] = njjr_df0[price_cols] * njjr_df0[adj_col].iloc[-1] / njjr_df1[adj_col].iloc[0]
    njjo_df = pd.concat([njjr_df0, njjr_df1], axis=0)

    df = pd.concat([df,
                    qhfn_df, noad_df, ofuo_df, slki_df, nigy_df,
                    pqrc_df, tuce_df, trlk_df, snbn_df, prkd_df,
                    tnrs_df, prjj_df, prjs_df, molr_df, plwm_df,
                    njjo_df], axis=0)
    df = df.sort_index()
    return df
