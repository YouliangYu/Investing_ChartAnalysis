from pandas_datareader import data
from datetime import datetime
import matplotlib

nvda = data.DataReader('NVDA', 'yahoo',datetime(2016,11,1),datetime(2016,12,30))
print(nvda['Adj Close'])