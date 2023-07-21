import eikon as ek
import myEikon as mek

# FTSE100, FTSE250, FTSE350, FTSE All-Share
indices = ['FTSE', 'FTMC', 'FTLC', 'FTAS', 'SP500', 'NDX', 'STOXX50E', 'STOXXE']

fetchQ = True
if fetchQ:
    for anindex in indices:
        ind = mek.Company('.'+anindex)
        price_df = ind.fetch_price()
        price_df.to_csv(f'files/price_stuff/indices/{anindex}.csv')


