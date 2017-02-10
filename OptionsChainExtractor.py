
from bs4 import BeautifulSoup
import requests
import re
import numpy as np
import pandas as pd
import h5py
import datetime
import time

class NasdaqOptions(object):
    '''
    adapted from https://quantcorner.wordpress.com/2015/11/02/fetching-nasdaq-options-data-with-python/
    Class NasdaqOptions fetches options data from Nasdaq website

    User inputs:
        Ticker: ticker
            - Ticker for the underlying
        Expiry: nearby
            - 1st Nearby: 1
            - 2nd Nearby: 2
            - etc ...
        Moneyness: money
            - All moneyness: all
            - In-the-money: in
            - Out-of-the-money: out
            - Near the money: near
        Market: market
            - Composite quote: Composite
            - Chicago Board Options Exchange: CBO
            - American Options Exchange: AOE
            - New York Options Exchange: NYO
            - Philadelphia Options Exchange: PHO
            - Montreal Options Exchange: MOE
            - Boston Options Exchange: BOX
            -  International Securities Exchange: ISE
            - Bats Exchange Options Market: BTO
            - NASDAQ Options: NSO
            - C2(Chicago) Options Exchange: C2O
            - NASDAQ OMX BX Options Exchange: BXO
            - MIAX: MIAX
        Option category: expir
            - Weekly options: week
            - Monthly options: stand
            - Quarterly options: quart
            - CEBO options (Credit Event Binary Options): cebo
    '''

    def __init__(self, ticker, nearby, market='composite', money='near', expir='all'):
        self.ticker = ticker
        self.nearby = nearby - 1  # ' refers 1st nearby on NASDAQ website
        self.market = market
        self.expir = expir
        self.money = money

    def get_options_table(self):
        #Get the complete option table and return a pandas.DataFrame() object.

        #Create an empty pandas.Dataframe object, to which new data will be appended.
        old_df = pd.DataFrame()

        # Variables
        loop = 0  # Loop over webpages starts at 0
        page_nb = 1  # Get the top of the options table
        flag = 1  # Set a flag that will be used to call get_pager()

        # Loop over webpages
        while loop < int(page_nb):
            # Construct the URL
            url = 'http://www.nasdaq.com/symbol/' + self.ticker + '/option-chain?excode=' + self.market + '&money=' + self.money + '&expir=' + self.expir + \
                  '&dateindex=' + str(self.nearby) + '&page=' + str(loop + 1)
            print(url)

            # Query NASDAQ website
            try:
                response = requests.get(url)
            # DNS lookup failure
            except requests.exceptions.ConnectionError as e:
                print('''Webpage doesn't seem to exist!\n%s''' % e)
                pass
            # Timeout failure
            except requests.exceptions.ConnectTimeout as e:
                print('''Slow connection!\n%s''' % e)
                pass
            # HTTP error
            except requests.exceptions.HTTPError as e:
                print('''HTTP error!\n%s''' % e)
                pass

            # Get webpage content
            soup = BeautifulSoup(response.content, 'html.parser')

            # Determine actual number of pages of the option table for nearby expiry to loop over
            if flag == 1:
                last_page_raw = soup.find('a', {'id': 'quotes_content_left_lb_LastPage'}) # <a> tag defines a hyperlink
                last_page = re.findall(pattern='(?:page=)(\d+)', string=str(last_page_raw))
                page_nb = ''.join(last_page)
                if page_nb == '':
                    page_nb = 1
                flag = 0

            # Extract table containing the option data as a list
            table = soup.find_all('table')[5]
            elems = table.find_all('td')  # <td> tag defines a standard cell in an HTML table
            lst = [elem.text for elem in elems]  # Option data as a readable list

            # Create a pandas.DataFrame
            arr = np.array(lst)
            reshaped = arr.reshape((int(len(lst)/16), 16))
            new_df = pd.DataFrame(reshaped)
            old_df = pd.concat([old_df, new_df])

            loop += 1

        # Name the columns
        old_df.columns =  ['Expire','Call_Last', 'Call_Chg', 'Call_Bid', 'Call_Ask', 'Call_Vol', 'Call_OI','Symbol','Strike',
                                'Expire_Puts','Put_Last', 'Put_Chg', 'Put_Bid', 'Put_Ask', 'Put_Vol', 'Put_OI']
        old_df = old_df.drop(['Symbol','Expire_Puts'], axis=1)

        return old_df

if __name__ == '__main__':
    t0 = time.time()
    for symbol in ['BIDU','INTC','AMD','NVDA','TSLA','FB','BABA','AAPL','AMZN','GOOG','IBM','GLD','SPY','QQQ']:
        for i in range(1,4):
#            money = 'near'
#            options = NasdaqOptions(symbol,i,money=money)
#            option_chain = options.get_options_table()
#            file = '~/computing/investing/OptionBackTester/Data_NearMoney/'+symbol+'_'+money+'_money_'+str(datetime.date.today())+'_'+str(i)+'.csv'
#            option_chain.to_csv(file,index=False)
            money = 'all'
            options = NasdaqOptions(symbol,i,money=money)
            option_chain = options.get_options_table()
            file = '~/computing/investing/OptionBackTester/Data/'+symbol+'_'+money+'_money_'+str(datetime.date.today())+'_'+str(i)+'.csv'
            option_chain.to_csv(file,index=False)

    print(time.time() - t0, "seconds wall time")
