import pandas as pd
import matplotlib.pyplot as plt
import useful_stuff

the_dir = 'files/price_stuff/adj_price_data_fixed/'
files = useful_stuff.listdir(the_dir)
count = 0
for afile in files:
    df = pd.read_csv(the_dir + afile)
    df = df.set_index('Date')
    df = df[['OPEN', 'HIGH', 'LOW', 'CLOSE']]
    df.plot()
    plt.title(afile)
    plt.yscale('log')
    plt.show()
    count += 1
    if count > 100:
        break
