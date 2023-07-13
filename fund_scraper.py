import pandas as pd
import eikon as ek
import myEikon as mek

rics = ["AZN.L", "HSBA.L"]
shits = pd.read_csv('./files/tl_table/supp_data.csv')
tl_dict = {}
fields = shits['TR_name'].to_list()
for i in range(len(shits)):
    tl_dict[shits['TR_name'][i]] = shits['shitty_name'][i]

comps = mek.Companies(rics)
comps.fetch_data(fields)
for ric in rics:
    comps.comp_specific_data(ric).to_csv(f'./fund_data/{ric}.csv')
