import yfinance as yf
import pandas as pd
from generic_pull import generic_pull
from datetime import datetime, timedelta
from generic_panda_db import generic_panda_db
import stock_price
from ordered_set import OrderedSet

class stock_price_pull(generic_panda_db):

    KEY_COLUMNS = ('Date', 'Symbol')
    columns = ('Date', 'Symbol', 'Price')

    def __get_key_columns__(self):
        return stock_price_pull.KEY_COLUMNS

    def __get_db_filename__(self):
        return "stock_data.db"

    def get_new_data(self, items, start, end):
        return yf.download(items, start = start, end = end)['Adj Close']

    # is the date ranage in the index
    def data_in(self, symbol, date_start, date_end, columns):
        unindexed = self.base_data.reset_index()
        #Check that all columns are 
        for col in columns:
            if col not in unindexed.columns:
                print ('column %s not in stored data')
                return False
        # check that all rows are in
        symbol = unindexed.loc[unindexed['Symbol'] == symbol]
        first_date = symbol.iloc[0]['Date']
        last_date = symbol.iloc[-1]['Date']
        return int(date_start.timestamp()) >= first_date and int(date_end.timestamp()) <= last_date
        

    def get_stock_price(self, symbols, start, end, dropna = True):
        '''
        Return pandas data frame where columns are the symbols and rows are dates
        
        symbols - list of stock symbols to lookup
        date_range - range of dates to retrieve
        '''
        # find cached values or create new data frames
        dates = pd.date_range(pd.Timestamp(start), pd.Timestamp(end))
        date_set = set(dates)

        start = datetime.strptime(start, "%Y-%m-%d")
        end = datetime.strptime(end, "%Y-%m-%d")
        daterange = pd.date_range(pd.Timestamp(start), pd.Timestamp(end))
        
        # check to see if the data is already in the database
        
        symbols_to_get = []
        '''
        for symbol in symbols:
            temp_df = self.base_data.reset_index()
            
            new_dates = pd.DataFrame({'Date': daterange, 'Symbol': [symbol] * len(daterange)})
            
            if not self.data_in(symbol, start, end, stock_price_pull.columns):
                symbols_to_get.append(symbol)
        '''
        #symbols_to_get = symbols
        if 1: #len(symbols_to_get) > 0:
            new_data = self.get_new_data(symbols, start, end)
            
            for symbol in symbols:
                nd = new_data[symbol].reindex(daterange, method='pad')
                new_df = pd.DataFrame({'Price': nd, 'Symbol' : [symbol] * len(nd), 'Date': daterange})
                
                self.insert_data(new_df)
        else:
            print ('Data already exists. No need to repull.')



    def __init__(self, ):
        super().__init__()
        # create a new column which is a date object
        uni =self.base_data.reset_index()
        #self.insert_data(pd.DataFrame({'Date': uni['Date'], 'Symbol': uni['Symbol'], 'data_obj' : [pd.Timestamp(date) for date in uni['Date']]}))
        self.get_stock_price(['MSFT', 'INTC'], "2017-01-01", "2017-04-28")

        
        
if __name__ ==  '__main__':
    spp = stock_price_pull()
    print (spp.base_data)