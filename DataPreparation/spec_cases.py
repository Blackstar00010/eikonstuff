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
    qhfn_df3 = qhfn_df2[qhfn_df2[date_col] <= '2020-05-12']
    qhfn_df4 = qhfn_df2[~qhfn_df2.index.isin(qhfn_df3.index)]
    qhfn_df4.loc[:, shrout_col] = qhfn_df3[shrout_col].iloc[-1]
    qhfn_df = pd.concat([qhfn_df0, qhfn_df1, qhfn_df3, qhfn_df4], axis=0)

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
    tnrs_df1.loc[:, price_cols] = tnrs_df1[price_cols] / tnrs_df1[close_col].iloc[0] * (
            tnrs_df0[shrout_col].iloc[-1] * tnrs_df0[close_col].iloc[-1] / tnrs_df1[shrout_col].iloc[0])
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

    ftrq_df = df[df['gvkey'] == us.ric2num('FTRQ')]
    df = df[df['gvkey'] != us.ric2num('FTRQ')]
    ftrq_df0 = ftrq_df[ftrq_df[date_col] <= '2010-01-08']
    ftrq_df = ftrq_df[~ftrq_df.index.isin(ftrq_df0.index)]
    ftrq_df1 = ftrq_df[ftrq_df[date_col] <= '2010-02-04']
    ftrq_df2 = ftrq_df[~ftrq_df.index.isin(ftrq_df1.index)]
    ftrq_df1.loc[:, close_col] = (ftrq_df0[close_col].iloc[-1] + ftrq_df2[close_col].iloc[0]) / 2
    ftrq_df1.loc[:, price_cols].iloc[0] = ftrq_df1.loc[:, close_col].iloc[0]
    ftrq_df = pd.concat([ftrq_df0, ftrq_df1, ftrq_df2], axis=0)

    dmxc_df = df[df['gvkey'] == us.ric2num('DMXC')]
    df = df[df['gvkey'] != us.ric2num('DMXC')]
    dmxc_df0 = dmxc_df[dmxc_df[date_col] <= '2004-05-21']
    dmxc_df = dmxc_df[~dmxc_df.index.isin(dmxc_df0.index)]
    dmxc_df1 = dmxc_df[dmxc_df[date_col] <= '2004-08-17']
    dmxc_df2 = dmxc_df[~dmxc_df.index.isin(dmxc_df1.index)]
    dmxc_df1.loc[:, price_cols] = dmxc_df1[price_cols] * 100
    dmxc_df = pd.concat([dmxc_df0, dmxc_df1, dmxc_df2], axis=0)

    fuyp_df = df[df['gvkey'] == us.ric2num('FUYP')]
    df = df[df['gvkey'] != us.ric2num('FUYP')]
    fuyp_df.loc[:, adj_col] = fuyp_df[adj_col].replace([8, 64], 1)
    fuyp_df0 = fuyp_df[fuyp_df[date_col] == '2008-12-05']
    fuyp_df = fuyp_df[~fuyp_df.index.isin(fuyp_df0.index)]
    fuyp_df0.loc[:, price_cols] = float('NaN')
    fuyp_df = pd.concat([fuyp_df0, fuyp_df], axis=0)

    gjwe_df = df[df['gvkey'] == us.ric2num('GJWE')]
    df = df[df['gvkey'] != us.ric2num('GJWE')]
    gjwe_df0 = gjwe_df[gjwe_df[date_col] <= '2004-05-21']
    gjwe_df = gjwe_df[~gjwe_df.index.isin(gjwe_df0.index)]
    gjwe_df1 = gjwe_df[gjwe_df[date_col] <= '2004-08-17']
    gjwe_df2 = gjwe_df[~gjwe_df.index.isin(gjwe_df1.index)]
    gjwe_df1.loc[:, close_col] = (gjwe_df0[close_col].iloc[-1] + gjwe_df2[close_col].iloc[0]) / 2
    gjwe_df = pd.concat([gjwe_df0, gjwe_df1, gjwe_df2], axis=0)

    xjx_df = df[df['gvkey'] == us.ric2num('XJX')]
    df = df[df['gvkey'] != us.ric2num('XJX')]
    xjx_df0 = xjx_df[xjx_df[date_col] <= '2007-12-19']
    xjx_df = xjx_df[~xjx_df.index.isin(xjx_df0.index)]
    xjx_df1 = xjx_df[xjx_df[date_col] <= '2008-01-04']
    xjx_df2 = xjx_df[~xjx_df.index.isin(xjx_df1.index)]
    xjx_df1.loc[:, close_col] = (xjx_df0[close_col].iloc[-1] + xjx_df2[close_col].iloc[0]) / 2
    xjx_df1.loc[:, price_cols].iloc[0] = xjx_df1.loc[:, close_col].iloc[0]
    xjx_df = pd.concat([xjx_df0, xjx_df1, xjx_df2], axis=0)

    fseo_df = df[df['gvkey'] == us.ric2num('FSEO')]
    df = df[df['gvkey'] != us.ric2num('FSEO')]
    fseo_df0 = fseo_df[fseo_df[date_col] <= '2010-06-08']
    fseo_df = fseo_df[~fseo_df.index.isin(fseo_df0.index)]
    fseo_df1 = fseo_df[fseo_df[date_col] <= '2012-02-09']
    fseo_df2 = fseo_df[~fseo_df.index.isin(fseo_df1.index)]
    fseo_df1.loc[:, price_cols] = fseo_df1[price_cols].replace(fseo_df1[close_col].iloc[0],
                                                               (fseo_df0[close_col].iloc[-1] +
                                                                fseo_df2[close_col].iloc[0]) / 2)
    fseo_df = pd.concat([fseo_df0, fseo_df1, fseo_df2], axis=0)

    skoy_df = df[df['gvkey'] == us.ric2num('SKOY')]
    df = df[df['gvkey'] != us.ric2num('SKOY')]
    skoy_df0 = skoy_df[skoy_df[date_col] <= '2019-09-25']
    skoy_df = skoy_df[~skoy_df.index.isin(skoy_df0.index)]
    skoy_df1 = skoy_df[skoy_df[date_col] <= '2019-10-10']
    skoy_df2 = skoy_df[~skoy_df.index.isin(skoy_df1.index)]
    skoy_df1.loc[:, shrout_col] = skoy_df0[shrout_col].iloc[-1]
    skoy_df = pd.concat([skoy_df0, skoy_df1, skoy_df2], axis=0)

    xoc_df = df[df['gvkey'] == us.ric2num('XOC')]
    df = df[df['gvkey'] != us.ric2num('XOC')]
    xoc_df0 = xoc_df[xoc_df[date_col] <= '2012-06-19']
    xoc_df = xoc_df[~xoc_df.index.isin(xoc_df0.index)]
    xoc_df1 = xoc_df[xoc_df[date_col] <= '2019-06-29']
    xoc_df2 = xoc_df[~xoc_df.index.isin(xoc_df1.index)]
    xoc_df1.loc[:, shrout_col] = xoc_df0[shrout_col].iloc[-1]
    xoc_df = pd.concat([xoc_df0, xoc_df1, xoc_df2], axis=0)

    wyr_df = df[df['gvkey'] == us.ric2num('WYR')]
    df = df[df['gvkey'] != us.ric2num('WYR')]
    wyr_df0 = wyr_df[wyr_df[date_col] <= '1994-11-25']
    wyr_df = wyr_df[~wyr_df.index.isin(wyr_df0.index)]
    wyr_df1 = wyr_df[wyr_df[date_col] <= '2007-11-27']
    wyr_df2 = wyr_df[~wyr_df.index.isin(wyr_df1.index)]
    wyr_df1.loc[:, price_cols] = wyr_df1[price_cols] / wyr_df1[close_col].iloc[-1] * wyr_df2[close_col].iloc[0]
    wyr_df0.loc[:, price_cols] = wyr_df0[price_cols] / wyr_df0[close_col].iloc[-1] * wyr_df1[close_col].iloc[0]
    wyr_df = pd.concat([wyr_df0, wyr_df1, wyr_df2], axis=0)

    fucp_df = df[df['gvkey'] == us.ric2num('FUCP')]
    df = df[df['gvkey'] != us.ric2num('FUCP')]
    fucp_df0 = fucp_df[fucp_df[date_col] <= '2020-05-22']
    fucp_df = fucp_df[~fucp_df.index.isin(fucp_df0.index)]
    fucp_df1 = fucp_df[fucp_df[date_col] <= '2020-05-26']
    fucp_df2 = fucp_df[~fucp_df.index.isin(fucp_df1.index)]
    fucp_df1.loc[:, price_cols] = float('NaN')
    fucp_df = pd.concat([fucp_df0, fucp_df1, fucp_df2], axis=0)

    nmzf_df = df[df['gvkey'] == us.ric2num('NMZF')]
    df = df[df['gvkey'] != us.ric2num('NMZF')]
    nmzf_df0 = nmzf_df[nmzf_df[date_col] <= '2018-08-16']
    nmzf_df1 = nmzf_df[~nmzf_df.index.isin(nmzf_df0.index)]
    nmzf_df1.loc[:, shrout_col].iloc[0] = nmzf_df1.loc[:, shrout_col].iloc[3]
    nmzf_df0.loc[:, adj_col] = nmzf_df0[adj_col] / nmzf_df0[shrout_col].iloc[-1] * nmzf_df1[shrout_col].iloc[0]
    nmzf_df = pd.concat([nmzf_df0, nmzf_df1], axis=0)

    nfoy_df = df[df['gvkey'] == us.ric2num('NFOY')]
    df = df[df['gvkey'] != us.ric2num('NFOY')]
    nfoy_df0 = nfoy_df[nfoy_df[date_col] <= '2022-06-24']
    nfoy_df1 = nfoy_df[~nfoy_df.index.isin(nfoy_df0.index)]
    nfoy_df0.loc[:, price_cols] = nfoy_df0[price_cols] / nfoy_df0[close_col].iloc[-1] * nfoy_df1[close_col].iloc[0]
    nfoy_df = pd.concat([nfoy_df0, nfoy_df1], axis=0)

    ryue_df = df[df['gvkey'] == us.ric2num('RYUE')]
    df = df[df['gvkey'] != us.ric2num('RYUE')]
    ryue_df0 = ryue_df[ryue_df[date_col] <= '2021-12-16']
    ryue_df1 = ryue_df[~ryue_df.index.isin(ryue_df0.index)]
    ryue_df0.loc[:, adj_col] = ryue_df0[adj_col] / ryue_df0[shrout_col].iloc[-1] * ryue_df1[shrout_col].iloc[0]
    ryue_df0.loc[:, price_cols] = ryue_df0[price_cols] / ryue_df0[close_col].iloc[-1] * ryue_df1[close_col].iloc[0] * \
                                  ryue_df0[adj_col].iloc[-1] / ryue_df1[adj_col].iloc[0]
    ryue_df = pd.concat([ryue_df0, ryue_df1], axis=0)

    nqca_df = df[df['gvkey'] == us.ric2num('NQCA')]
    df = df[df['gvkey'] != us.ric2num('NQCA')]
    nqca_df0 = nqca_df[nqca_df[date_col] <= '2015-05-21']
    nqca_df = nqca_df[~nqca_df.index.isin(nqca_df0.index)]
    nqca_df1 = nqca_df[nqca_df[date_col] <= '2015-05-25']
    nqca_df2 = nqca_df[~nqca_df.index.isin(nqca_df1.index)]
    nqca_df1.loc[:, price_cols].iloc[0] = nqca_df1[price_cols].iloc[0] / nqca_df1[close_col].iloc[0] * \
                                          nqca_df0[close_col].iloc[-1]
    nqca_df1.loc[:, shrout_col] = nqca_df0[shrout_col].iloc[-1]
    nqca_df1.loc[:, adj_col] = nqca_df0[adj_col].iloc[-1]
    nqca_df = pd.concat([nqca_df0, nqca_df1, nqca_df2], axis=0)

    qfoq_df = df[df['gvkey'] == us.ric2num('QFOQ')]
    df = df[df['gvkey'] != us.ric2num('QFOQ')]
    qfoq_df0 = qfoq_df[qfoq_df[date_col] <= '2010-03-26']
    qfoq_df1 = qfoq_df[~qfoq_df.index.isin(qfoq_df0.index)]
    qfoq_df0[adj_col] = qfoq_df0[adj_col].replace(0, 0.05 * 0.002)
    qfoq_df0.loc[:, price_cols] = qfoq_df0[price_cols] * qfoq_df0[adj_col].iloc[-1] / qfoq_df1[adj_col].iloc[0]
    qfoq_df = pd.concat([qfoq_df0, qfoq_df1], axis=0)

    pxks_df = df[df['gvkey'] == us.ric2num('PXKS')]
    df = df[df['gvkey'] != us.ric2num('PXKS')]
    pxks_df0 = pxks_df[pxks_df[date_col] <= '2019-12-20']
    pxks_df1 = pxks_df[~pxks_df.index.isin(pxks_df0.index)]
    pxks_df0.loc[:, shrout_col] = pxks_df1[shrout_col].iloc[0]
    pxks_df = pd.concat([pxks_df0, pxks_df1], axis=0)

    mpgg_df = df[df['gvkey'] == us.ric2num('MPGG')]
    df = df[df['gvkey'] != us.ric2num('MPGG')]
    mpgg_df0 = mpgg_df[mpgg_df[date_col] <= '2010-10-04']
    mpgg_df1 = mpgg_df[~mpgg_df.index.isin(mpgg_df0.index)]
    mpgg_df0.loc[:, adj_col] = mpgg_df0[close_col].iloc[-1] / mpgg_df1[close_col].iloc[0] * mpgg_df1[adj_col].iloc[0]
    mpgg_df00 = mpgg_df0[mpgg_df0[date_col] < '1999-07-29']
    mpgg_df0 = mpgg_df0[mpgg_df0[date_col] >= '1999-07-29']
    mpgg_df00.loc[:, adj_col] = mpgg_df00[adj_col] / mpgg_df00[shrout_col].iloc[-1] * mpgg_df0[shrout_col].iloc[0]
    mpgg_df000 = mpgg_df00[mpgg_df00[date_col] < '1999-06-23']
    mpgg_df00 = mpgg_df00[mpgg_df00[date_col] >= '1999-06-23']
    mpgg_df000.loc[:, adj_col] = mpgg_df000[adj_col] / mpgg_df000[shrout_col].iloc[-1] * mpgg_df00[shrout_col].iloc[0]
    mpgg_df = pd.concat([mpgg_df000, mpgg_df00, mpgg_df0, mpgg_df1], axis=0)

    ohde_df = df[df['gvkey'] == us.ric2num('OHDE')]
    df = df[df['gvkey'] != us.ric2num('OHDE')]
    ohde_df0 = ohde_df[ohde_df[date_col] <= '2022-12-15']
    ohde_df1 = ohde_df[~ohde_df.index.isin(ohde_df0.index)]
    ohde_df0.loc[:, adj_col] = ohde_df1[adj_col].iloc[0] * ohde_df1[shrout_col].iloc[0] / ohde_df0[shrout_col]
    ohde_df = pd.concat([ohde_df0, ohde_df1], axis=0)

    # todo
    # mcvb_df = df[df['gvkey'] == us.ric2num('MCVB')]
    df = df[df['gvkey'] != us.ric2num('MCVB')]
    # mcvb_df0 = mcvb_df[mcvb_df[date_col] <= '2021-04-13']
    # mcvb_df = mcvb_df[~mcvb_df.index.isin(mcvb_df0.index)]
    # mcvb_df1 = mcvb_df[mcvb_df[date_col] <= '2021-07-14']
    # mcvb_df2 = mcvb_df[~mcvb_df.index.isin(mcvb_df1.index)]
    # mcvb_df1.loc[:, adj_col] = 1/mcvb_df0[adj_col].iloc[-1]
    # mcvb_df1.loc[:, shrout_col] = mcvb_df2[shrout_col].iloc[0] * mcvb_df2[adj_col].iloc[0] / mcvb_df1[adj_col].iloc[-1]
    # mcvb_df0.loc[:, adj_col] = mcvb_df0[adj_col] * mcvb_df1[adj_col].iloc[0]
    # mcvb_df0.loc[:, shrout_col] = mcvb_df0[shrout_col] * mcvb_df1[adj_col].iloc[0]
    # mcvb_df00 = mcvb_df0[mcvb_df0[date_col] <= '2021-04-05']
    # mcvb_df0 = mcvb_df0[~mcvb_df0.index.isin(mcvb_df00.index)]
    # mcvb_df00.loc[:, price_cols] = mcvb_df00[price_cols] / mcvb_df00[close_col].iloc[-1] * mcvb_df0[close_col].iloc[0]
    # mcvb_df000 = mcvb_df00[mcvb_df00[date_col] <= '2015-12-11']
    # mcvb_df00 = mcvb_df00[~mcvb_df00.index.isin(mcvb_df000.index)]
    # mcvb_df000.loc[:, price_cols] = mcvb_df000[price_cols] / 5
    # mcvb_df0000 = mcvb_df000[mcvb_df000[date_col] <= '2015-12-01']
    # mcvb_df000 = mcvb_df000[~mcvb_df000.index.isin(mcvb_df0000.index)]
    # mcvb_df0000.loc[:, close_col].iloc[-1] = mcvb_df0000.loc[:, close_col].iloc[-2]
    # mcvb_df0000.loc[:, adj_col] = mcvb_df000[adj_col].iloc[0] / mcvb_df000[adj_col].iloc[0] * mcvb_df0000[adj_col].iloc[-1]

    # todo
    df = df[df['gvkey'] != us.ric2num('BKJO')]

    nksx_df = df[df['gvkey'] == us.ric2num('NKSX')]
    df = df[df['gvkey'] != us.ric2num('NKSX')]
    nksx_df0 = nksx_df[nksx_df[date_col] <= '2008-02-25']
    nksx_df1 = nksx_df[~nksx_df.index.isin(nksx_df0.index)]
    nksx_df0.loc[:, adj_col] = nksx_df1[adj_col].iloc[0] * nksx_df1[shrout_col].iloc[0] / nksx_df0[shrout_col].iloc[-1]
    nksx_df1[adj_col].iloc[0] = nksx_df1[adj_col].iloc[1]
    nksx_df0.loc[:, shrout_col] = (nksx_df1[shrout_col].iloc[0] * nksx_df1[adj_col].iloc[0] / nksx_df0[adj_col].iloc[-1]) / nksx_df0[shrout_col].iloc[-1] * nksx_df0[shrout_col]
    nksx_df = pd.concat([nksx_df0, nksx_df1], axis=0)

    nrdi_df = df[df['gvkey'] == us.ric2num('NRDI')]
    df = df[df['gvkey'] != us.ric2num('NRDI')]
    nrdi_df0 = nrdi_df[nrdi_df[date_col] <= '2011-02-21']
    nrdi_df1 = nrdi_df[~nrdi_df.index.isin(nrdi_df0.index)]
    nrdi_df0.loc[:, shrout_col] = nrdi_df0[shrout_col] / nrdi_df0[shrout_col].iloc[-1] * nrdi_df1[shrout_col].iloc[0]
    nrdi_df = pd.concat([nrdi_df0, nrdi_df1], axis=0)

    opyo_df = df[df['gvkey'] == us.ric2num('OPYO')]
    df = df[df['gvkey'] != us.ric2num('OPYO')]
    opyo_df0 = opyo_df[opyo_df[date_col] <= '2018-04-03']
    opyo_df1 = opyo_df[~opyo_df.index.isin(opyo_df0.index)]
    opyo_df0.loc[:, adj_col] = opyo_df1[adj_col].iloc[0] * opyo_df1[shrout_col].iloc[0] / opyo_df0[shrout_col].iloc[-1]
    opyo_df0.loc[:, price_cols] = (opyo_df0[price_cols] /
                                   ((opyo_df0[shrout_col].iloc[-1] * opyo_df0[close_col].iloc[-1]) /
                                    (opyo_df1[shrout_col].iloc[0] * opyo_df1[close_col].iloc[0])))
    opyo_df2 = opyo_df1[opyo_df1[date_col] > '2018-05-21']
    opyo_df1 = opyo_df1[opyo_df1[date_col] <= '2018-05-21']
    opyo_df2.loc[:, shrout_col] = opyo_df2[shrout_col] / opyo_df2[shrout_col].iloc[0] * opyo_df1[shrout_col].iloc[-1]
    opyo_df = pd.concat([opyo_df0, opyo_df1, opyo_df2], axis=0)

    ltig_df = df[df['gvkey'] == us.ric2num('LTIG')]
    df = df[df['gvkey'] != us.ric2num('LTIG')]
    ltig_df0 = ltig_df[ltig_df[date_col] <= '1999-07-08']
    ltig_df1 = ltig_df[~ltig_df.index.isin(ltig_df0.index)]
    ltig_df0.loc[:, adj_col] = ltig_df1[adj_col].iloc[0] * ltig_df1[shrout_col].iloc[0] / ltig_df0[shrout_col].iloc[-1]
    ltig_df = pd.concat([ltig_df0, ltig_df1], axis=0)

    oayq_df = df[df['gvkey'] == us.ric2num('OAYQ')]
    df = df[df['gvkey'] != us.ric2num('OAYQ')]
    oayq_df0 = oayq_df[oayq_df[date_col] <= '2011-08-19']
    oayq_df1 = oayq_df[~oayq_df.index.isin(oayq_df0.index)]
    oayq_df0.loc[:, adj_col] = (oayq_df0[adj_col] *
                                ((oayq_df1[adj_col].iloc[0] / oayq_df1[shrout_col].iloc[0]) /
                                 (oayq_df0[adj_col].iloc[-1] / oayq_df0[shrout_col].iloc[-1])))
    oayq_df0.loc[:, price_cols] = oayq_df0.loc[:, price_cols].replace(0.0023, 0.9742)
    oayq_df1.loc[:, price_cols] = oayq_df1.loc[:, price_cols].replace(0.0024, 0.0206)
    oayq_df = pd.concat([oayq_df0, oayq_df1], axis=0)

    df = pd.concat([df,
                    qhfn_df, noad_df, ofuo_df, slki_df, nigy_df,
                    pqrc_df, tuce_df, trlk_df, snbn_df, prkd_df,
                    tnrs_df, prjj_df, prjs_df, molr_df, plwm_df,
                    njjo_df, ftrq_df, dmxc_df, fuyp_df, gjwe_df,
                    xjx_df, fseo_df, skoy_df, xoc_df, wyr_df,
                    fucp_df, nmzf_df, nfoy_df, ryue_df, nqca_df,
                    qfoq_df, pxks_df, mpgg_df, ohde_df,
                    nksx_df, nrdi_df, opyo_df, oayq_df], axis=0)
    df = df.sort_index()
    return df
