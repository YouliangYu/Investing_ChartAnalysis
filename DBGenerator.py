from pandas_datareader import data
import datetime
import pandas as pd
import numpy as np
import sqlite3 as sql
import os
from itertools import zip_longest
import time
from pandas.tseries.offsets import BDay

def add_to_database(init = 0, ticker='', close_date = ''):

    '''get raw data with two nearby options chain'''
    file1 = '/home/youliang/computing/investing/OptionBackTester/Data/'+ticker+'_all_money_'+close_date+'_1.csv'
    file2 = '/home/youliang/computing/investing/OptionBackTester/Data/'+ticker+'_all_money_'+close_date+'_2.csv'
    file3 = '/home/youliang/computing/investing/OptionBackTester/Data/'+ticker+'_all_money_'+close_date+'_3.csv' # added at a later time

#    if os.path.isfile(file1) and os.path.isfile(file2) or os.path.isfile(file3):
    if os.path.isfile(file1) and os.path.isfile(file2):
         pass
    else:
        print('equity '+ticker+': options chain at_'+close_date+' not exists, check again!')
        return

    if os.path.isfile(file3):
        option_data = pd.concat([pd.read_csv(file1),pd.read_csv(file2),pd.read_csv(file3)],ignore_index=True)
    else:
        option_data = pd.concat([pd.read_csv(file1),pd.read_csv(file2)],ignore_index=True)

    if init == 1:
        os.remove("OptionsChain.db")
        conn = sql.connect('OptionsChain.db')
        c = conn.cursor()

        '''create relational tables'''
        c.execute('''CREATE TABLE `Dates` (
                `id`	INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                `close_date`	TEXT)''')

        c.execute('''CREATE TABLE `Expiry` (
                `id`	INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                `expiry_date`	TEXT)''')

        c.execute('''CREATE TABLE `Strike` (
                `id`	INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                `strike_price`	NUMERIC)''')

        c.execute('''CREATE TABLE `Symbol` (
                `id`	INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                `ticker`	TEXT)''')

        c.execute('''CREATE TABLE `OptionsChain` (
                `symbol_id` INTEGER,
                `date_id`	INTEGER,
                `expiry_id`	INTEGER,
                `strike_id`	INTEGER,
                `call_mark`	NUMERIC,
                `call_bid`	NUMERIC,
                `call_ask`	NUMERIC,
                `call_vol`	INTEGER,
                `put_mark`	NUMERIC,
                `put_bid`	NUMERIC,
                `put_ask`	NUMERIC,
                `put_vol`	INTEGER)''')

    else:
        conn = sql.connect('OptionsChain.db')
        c = conn.cursor()

    if init == 1:

        '''modify Options Chain'''
        # get market value
        market_call = 0.5*(option_data['Call_Bid']+option_data['Call_Ask'])
        option_data['Call_Last'] = np.copy(market_call)
        market_put = 0.5*(option_data['Put_Bid']+option_data['Put_Ask'])
        option_data['Put_Last'] = np.copy(market_put)

        # drop nan
        index_call = list(market_call.index[market_call.apply(np.isnan)])
        index_put = list(market_put.index[market_put.apply(np.isnan)])
        index_drop = list(set(index_call).intersection(set(index_put)))
        option_data = option_data.drop(index_drop)

        '''insert table Dates'''
        c.execute("insert into Dates(close_date) values (?)",(close_date,))

        '''insert table Symbol'''
        c.execute("insert into Symbol(ticker) values (?)",(ticker,))

        '''insert table Expiry'''
        expiry_date = option_data['Expire'].unique().tolist()

        insert = [(i,) for i in expiry_date]
        c.executemany("insert into Expiry (expiry_date) values (?)", insert)

        '''insert table Strike'''
        strike_price = option_data['Strike'].unique().tolist()
        insert = [(item,) for item in strike_price]
        c.executemany("insert into Strike (strike_price) values (?)", insert)

        # id expiry_date
        c.execute("select expiry_date from Expiry")
        expiry_exist = list(c.fetchall())
        for item in expiry_date:
            option_data['Expire'].replace(item, expiry_exist.index((item,))+1, inplace = True)
        # id close_date
        c.execute("select close_date from Dates")
        add_date = (c.fetchall().index((close_date,))+1)*np.ones((option_data.shape[0],), dtype=np.int)
        option_data['Close_Date'] = pd.Series(add_date, index=option_data.index)
        # id strike_price
        c.execute("select strike_price from Strike")
        strike_price_exist = c.fetchall()
        for item in strike_price:
            option_data['Strike'].replace(item, str(strike_price_exist.index((item,))+1), inplace = True)

        option_data['Strike'] = option_data['Strike'].astype('int32', copy=True, raise_on_error=True)
#        print(option_data['Strike'].head(10))

        # id ticker
        c.execute("select ticker from Symbol")
        add_ticker = (c.fetchall().index((ticker,))+1)*np.ones((option_data.shape[0],), dtype=np.int)
        option_data['Symbol'] = pd.Series(add_ticker, index=option_data.index)

        '''insert table OptionsChain'''
        symbol_id = option_data['Symbol'].tolist()
        date_id = option_data['Close_Date'].tolist()
        expiry_id = option_data['Expire'].tolist()
        strike_id = option_data['Strike'].tolist()
        insert_cm = option_data['Call_Last'].tolist()
        insert_cb = option_data['Call_Bid'].tolist()
        insert_ca = option_data['Call_Ask'].tolist()
        insert_cv = option_data['Call_Vol'].tolist()
        insert_pm = option_data['Put_Last'].tolist()
        insert_pb = option_data['Put_Bid'].tolist()
        insert_pa = option_data['Put_Ask'].tolist()
        insert_pv = option_data['Put_Vol'].tolist()

        #print(list(zip_longest(symbol_id,date_id,expiry_id,strike_id,insert_cm,insert_cb,insert_ca,insert_cv,insert_pm,insert_pb,insert_pa,insert_pv)))

        c.executemany("INSERT into OptionsChain VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                    zip_longest(symbol_id,date_id,expiry_id,strike_id,insert_cm,insert_cb,insert_ca,insert_cv,insert_pm,insert_pb,insert_pa,insert_pv))
    else:

        '''modify Options Chain'''
        # get market value
        market_call = 0.5*(option_data['Call_Bid']+option_data['Call_Ask'])
        option_data['Call_Last'] = np.copy(market_call)
        market_put = 0.5*(option_data['Put_Bid']+option_data['Put_Ask'])
        option_data['Put_Last'] = np.copy(market_put)

        # drop nan
        index_call = list(market_call.index[market_call.apply(np.isnan)])
        index_put = list(market_put.index[market_put.apply(np.isnan)])
        index_drop = list(set(index_call).intersection(set(index_put)))
        option_data = option_data.drop(index_drop)

        '''insert table Dates'''
        c.execute("select close_date from Dates")
        date_exist = c.fetchall()
        if (close_date,) in date_exist:
            pass
        else:
            c.execute("insert into Dates(close_date) values (?)",(close_date,))

        '''insert table Symbol'''
        c.execute("select ticker from Symbol")
        ticker_exist = c.fetchall()
        if (ticker,) in ticker_exist:
            pass
        else:
            c.execute("insert into Symbol(ticker) values (?)",(ticker,))

        '''insert table Expiry'''
        c.execute("select expiry_date from Expiry")
        expiry_exist = c.fetchall()
        expiry_date = option_data['Expire'].unique().tolist()
        insert = [(item,) for item in expiry_date]
        for item in expiry_exist:
            if item in insert:
                del insert[insert.index(item)]
        if insert != []:
            c.executemany("insert into Expiry (expiry_date) values (?)", insert)

        #c.execute("select expiry_date from Expiry")
        #tmp = c.fetchall()
        #print(len(tmp),len(set(tmp))) # check duplicate date

        '''insert table Strike'''
        c.execute("select strike_price from Strike")
        strike_price_exist = c.fetchall()
#        strike_price = sorted(option_data['Strike'].unique().tolist())
        strike_price = option_data['Strike'].unique().tolist()

        insert = [(item,) for item in strike_price]
        for item in strike_price_exist:
            if item in insert:
                del insert[insert.index(item)]
        if insert != []:
            c.executemany("insert into Strike (strike_price) values (?)", insert)

        #c.execute("select strike_price from Strike")
        #tmp = c.fetchall()
        #print(len(tmp),len(set(tmp))) # check duplicate strike_price

        # id expiry_date
        c.execute("select expiry_date from Expiry")
        expiry_exist = c.fetchall()
        for item in expiry_date:
            option_data['Expire'].replace(item,expiry_exist.index((item,))+1, inplace = True)
        # id close_date
        c.execute("select close_date from Dates")
        add_date = (c.fetchall().index((close_date,))+1)*np.ones((option_data.shape[0],), dtype=np.int)
        option_data['Close_Date'] = pd.Series(add_date, index=option_data.index)
        # id strike_price
        c.execute("select strike_price from Strike")
        strike_price_exist = c.fetchall()
        for item in strike_price:
            option_data['Strike'].replace(item, str(strike_price_exist.index((item,))+1), inplace = True)
        option_data['Strike'] = option_data['Strike'].astype('int32', copy=True, raise_on_error=True)

        # id ticker
        c.execute("select ticker from Symbol")
        add_ticker = (c.fetchall().index((ticker,))+1)*np.ones((option_data.shape[0],), dtype=np.int)
        option_data['Symbol'] = pd.Series(add_ticker, index=option_data.index)

        '''insert table OptionsChain'''
        symbol_id = option_data['Symbol'].tolist()
        date_id = option_data['Close_Date'].tolist()
        expiry_id = option_data['Expire'].tolist()
        strike_id = option_data['Strike'].tolist()
        insert_cm = option_data['Call_Last'].tolist()
        insert_cb = option_data['Call_Bid'].tolist()
        insert_ca = option_data['Call_Ask'].tolist()
        insert_cv = option_data['Call_Vol'].tolist()
        insert_pm = option_data['Put_Last'].tolist()
        insert_pb = option_data['Put_Bid'].tolist()
        insert_pa = option_data['Put_Ask'].tolist()
        insert_pv = option_data['Put_Vol'].tolist()

        #print(list(zip_longest(symbol_id,date_id,expiry_id,strike_id,insert_cm,insert_cb,insert_ca,insert_cv,insert_pm,insert_pb,insert_pa,insert_pv)))

        c.executemany("INSERT into OptionsChain VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                    zip_longest(symbol_id,date_id,expiry_id,strike_id,insert_cm,insert_cb,insert_ca,insert_cv,insert_pm,insert_pb,insert_pa,insert_pv))

#    print(option_data.head())

    conn.commit()
    c.close()

if __name__ == '__main__':

    t0 = time.time()
    #initialize database with NVDA
#    add_to_database(init=1,ticker='NVDA',close_date = str(datetime.date(2017,1,13)))
#    print('Added NVDA at '+str(datetime.date(2017,1,13))+' to the database...')

    # add more tickers with more dates
#    for ticker in  ['INTC','AMD','TSLA','FB','BABA','AAPL','AMZN','GOOG','IBM','GLD','SPY','QQQ']:
#        add_to_database(init=2,ticker=ticker,close_date = str(datetime.date(2017,1,13)))
#        print('Added '+ticker+' at '+str(datetime.date(2017,1,13))+' to the database...')

    start = datetime.date.today()# datetime.date(2017,1,17)
    end = datetime.date.today() + datetime.timedelta(days=1) #datetime.date.today()
    daydiff = (end - start).days
    for i in range(daydiff):
        tmp_date = str(start + BDay(i))[:10]
        for ticker in  ['BIDU','INTC','AMD','NVDA','TSLA','FB','BABA','AAPL','AMZN','GOOG','IBM','GLD','SPY','QQQ']:
            add_to_database(init=2,ticker=ticker,close_date = tmp_date)
            print('Added '+ticker+' at '+tmp_date+' to the database...')

    print(time.time() - t0, "seconds wall time")
