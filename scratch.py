import pandas as pd
import math
import time
import myEikon as dbb
import eikon as ek

"""
print(ek.get_data("AZN.L", [{"TR.SharesOutstanding": {"SDate": "2010-01-01", "EDate": "2020-12-31", "Currency": "USD"}},
                            {"TR.NormIncomeBeforeTaxes": {"SDate": "2010-01-01", "EDate": "2020-12-31", "Currency": "USD"}},
                            {"TR.NormIncomeBeforeTaxes": {"SDate": "2010-01-01", "EDate": "2020-12-31"}},
                            "Currency"])[0].transpose())
"""
"""
shrout = ek.get_data(["AZN.L", "AAPL.O"], 'TR.SharesOutstanding', {"SDate": "2010-01-01", "EDate": "2020-12-31"})[0]
aznshrout = shrout[shrout["Instrument"] == "AZN.L"]
print(aznshrout)
prices = ek.get_timeseries(["AZN.L", "AAPL.O"], start_date="2010-01-01", end_date="2020-12-31")
aznprice = prices["AZN.L"]
print(aznprice)
"""

"""
print(ek.get_data(["AZN.L", "AAPL.O", "HSBA.L"], "Currency")[0])
"""

'''
asdf = pd.read_csv('https://www.jamesd002.com/file/close.csv')
print(asdf)
'''

'''
txtlist = ['230710_161343_log.txt', '230710_192414_log.txt', '230710_195715_log.txt', '230710_200219_log.txt']
shits = []
for txtfile in txtlist:
    with open('./files/' + txtfile, 'r') as f:
        loglines = [line.rstrip() for line in f]

    loglines = [i for i in loglines if len(i) > 0 and i[0] == "!"]
    loglines = [i.replace('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!Empty Dataset for ', '').replace(
        '!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!', '') for i in loglines]
    [shits.append(item) for item in loglines]
print(shits)

with open('./files/no_data.txt', 'w') as f:
    for line in shits:
        f.write(f"{line}\n")
'''

hsba = pd.read_csv('./files/fund_data/HSBA.L.csv')
print(hsba[hsba['Instrument'] == 'HSBA.L'])
