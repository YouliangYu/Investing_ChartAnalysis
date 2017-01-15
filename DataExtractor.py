from pandas_datareader import data
from datetime import datetime
import matplotlib
import h5py

#nvda = data.DataReader('NVDA', 'yahoo',datetime(2016,11,1),datetime(2016,12,30))
#print(nvda['Adj Close'].head())
aapl = data.Options('aapl', 'yahoo')
data = aapl.get_all_data()
