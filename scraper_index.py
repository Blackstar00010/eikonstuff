import os
import eikon as ek
import myEikon as mek
from scraper_price import fix_ohlccv
import pandas as pd

# FTSE100, FTSE250, FTSE350, FTSE All-Share
indices = ['FTSE', 'FTMC', 'FTLC', 'FTAS', 'SP500', 'NDX', 'STOXX50E', 'STOXXE']
output_dir = 'files/price_stuff/indices/'
merge_dir = 'files/price_stuff/indices_merged/'

fetchQ = True
if fetchQ:
    for anindex in indices:
        ind = mek.Company('.'+anindex)
        price_df = ind.fetch_price().reset_index()
        price_df = fix_ohlccv(price_df)
        price_df.to_csv(output_dir + anindex + '.csv', index=False)
        print(f'{anindex} fetched!')

mergeQ = True
if mergeQ:
    files = os.listdir(output_dir)
    df = pd.read_csv(output_dir + files[0])
    firm_name = files[0].split('.')[0]
    phigh = df[['Date', 'HIGH']].rename(columns={'HIGH': firm_name})
    plow = df[['Date', 'LOW']].rename(columns={'LOW': firm_name})
    popen = df[['Date', 'OPEN']].rename(columns={'OPEN': firm_name})
    pclose = df[['Date', 'CLOSE']].rename(columns={'CLOSE': firm_name})

    print('Merging price data...\n[', end="")
    for i, afile in enumerate(files[1:]):
        df = pd.read_csv(output_dir + afile)
        firm_name = afile.split('.')[0]
        phigh = phigh.merge(df[['Date', 'HIGH']], on="Date", how="outer").rename(columns={'HIGH': firm_name})
        plow = plow.merge(df[['Date', 'LOW']], on="Date", how="outer").rename(columns={'LOW': firm_name})
        popen = popen.merge(df[['Date', 'OPEN']], on="Date", how="outer").rename(columns={'OPEN': firm_name})
        pclose = pclose.merge(df[['Date', 'CLOSE']], on="Date", how="outer").rename(columns={'CLOSE': firm_name})

        if i % 5 == 3:
            print('-', end="")
        if i % 100 == 98:
            print(f']  {i + 2}/{len(files)}\n[', end="")
    print(f']  {len(files)}/{len(files)}')

    phigh = phigh.sort_values('Date').set_index('Date')
    plow = plow.sort_values('Date').set_index('Date')
    popen = popen.sort_values('Date').set_index('Date')
    pclose = pclose.sort_values('Date').set_index('Date')

    phigh = phigh.fillna(method='ffill')
    plow = plow.fillna(method='ffill')
    popen = popen.fillna(method='ffill')
    pclose = pclose.fillna(method='ffill')

    phigh.to_csv(merge_dir + 'index_high_d.csv', index=True)
    plow.to_csv(merge_dir + 'index_low_d.csv', index=True)
    popen.to_csv(merge_dir + 'index_open_d.csv', index=True)
    pclose.to_csv(merge_dir + 'index_close_d.csv', index=True)
    print('Finished merging price data!')

momQ = True  # month-on-month, not momentum
if momQ:
    for ohlc in ['open', 'high', 'low', 'close']:
        daily_df = pd.read_csv(merge_dir + f'index_{ohlc}_d.csv')
        month_end = ~(daily_df['Date'].str[5:7] == daily_df['Date'].str[5:7].shift(1))
        month_df = daily_df[month_end].set_index('Date')
        month_df.to_csv(merge_dir + f'index_{ohlc}_m.csv')

        ret_df = month_df/month_df.shift(-1) -1
        ret_df.to_csv(merge_dir + f'index_{ohlc}_mret.csv')

