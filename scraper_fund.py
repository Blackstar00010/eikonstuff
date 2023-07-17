import pandas as pd
import eikon as ek
import myEikon as mek

ref_comp_df = pd.read_csv('./files/tl_table/ref-comp.csv')
all_rics = ref_comp_df['ric'].to_list()
shits = pd.read_csv('./files/tl_table/supp_data.csv')
tl_dict = {}
fields = shits['TR_name'].to_list()
for i in range(len(shits)):
    tl_dict[shits['TR_name'][i].upper()] = shits['shitty_name'][i]
data_type = 'FY'


slice_by = 300
start_firm = 0
# split into smaller list just in case it might give some blank rows
for i in range(start_firm, len(all_rics), slice_by):
    rics = all_rics[i:i+slice_by]
    comps = mek.Companies(rics)

    # try until we can fetch some data
    goodenough = False
    while not goodenough:
        comps.fetch_data(fields, start='2000-01-01', period=data_type)
        if len(comps.get_history(-1)) > 0:
            goodenough = True
            df_21c = comps.get_history(-1)[0]
            comps.clear_history()
    goodenough = False
    while not goodenough:
        comps.fetch_data(fields, start='1980-01-01', end='1999-12-31', period=data_type)
        if len(comps.get_history(-1)) > 0:
            goodenough = True
            df_20c = comps.get_history(-1)[0]
            comps.set_history(pd.concat([df_21c, df_20c], axis=0))

    # separate, arrange, and save
    for ric in rics:
        # N=len(tr_dict) number of value columns, N number of date columns
        comp_df = comps.comp_specific_data(ric)
        # columns: ['Instrument', 'TR.STH1', 'TR.STH1.DATE', 'TR.STH2', 'TR.STH2.DATE', ...]

        comp_df_new = comp_df.loc[:, comp_df.columns[0:3]]  # first data
        comp_df_new = comp_df_new.rename(columns={comp_df_new.columns[-1]: 'datadate'})
        for acol in comp_df.columns[3:]:
            if acol[-9:] != '.CALCDATE':
                to_concat = comp_df.loc[:, [acol + ".CALCDATE", acol]]
                to_concat = to_concat.rename(columns={acol + ".CALCDATE": 'datadate'})
                comp_df_new = pd.concat([comp_df_new, to_concat])
        # manage duplicate 'datadate'
        comp_df_new = comp_df_new.groupby('datadate').sum().reset_index()

        # rename columns & add dcpstk column
        comp_df_new = comp_df_new.rename(columns=tl_dict)
        comp_df_new.loc[:, 'dcpstk'] = comp_df_new.loc[:, 'pstk'] + comp_df_new.loc[:, 'dcvt']
        if data_type == 'FQ':
            comp_df_new = comp_df_new.rename(columns={col: col + 'q' for col in comp_df_new.columns
                                                      if col not in ['datadate', 'Instrument']})

        # final cleanup
        comp_df_new = comp_df_new.drop(['Instrument'], axis=1)
        comp_df_new = comp_df_new.replace(0, float('NaN'))  # fill empty columns
        comp_df_new = comp_df_new.replace('', float('NaN'))  # fill empty columns
        comp_df_new = comp_df_new.fillna(method='ffill')    # fill empty columns
        comp_df_new = comp_df_new.dropna(how='all')
        if len(comp_df_new) > 0:
            comp_df_new.to_csv(f'./files/fund_data/{data_type}/{ric.replace(".", "-")}.csv', index=False)

        print(ric, 'done!')
    comps.get_history(-1)[0].to_csv(f'./files/fund_data/bunches/bunch-{data_type}-{str(i).zfill(5)}.csv', index=False)
    