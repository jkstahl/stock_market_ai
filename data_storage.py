import pandas as pd
import os, pickle

class GenericSeriesStorage():
    
    def __init__(self):
        self.cache_filename = self.__class__.__name__ + '.cache'
        if not os.path.exists(self.cache_filename):
            self.cache = {}
        else:
            self.load()
    
    def load(self):
        with open(self.cache_filename, 'rb') as fp:
            self.cache =  pickle.load(fp)
    
    def save(self):
        with open(self.cache_filename, 'wb') as fp:
            pickle.dump(self.cache, fp)
    
    def get_stock_price(self, symbols, start, end, dropna = True):
        '''
        Return pandas data frame where columns are the symbols and rows are dates
        
        symbols - list of stock symbols to lookup
        date_range - range of dates to retrieve
        '''
        # find cached values or create new data frames
        need_to_get = []
        dates = pd.date_range(pd.Timestamp(start), pd.Timestamp(end))
        date_set = set(dates)
        data = pd.DataFrame({s: [None] * len(dates) for s in symbols}, index = dates)
        for symbol in symbols:    
            if symbol in self.cache and len(date_set - set(self.cache[symbol].index) )== 0: 
                data[symbol] = self.cache[symbol]
            else:
                need_to_get.append(symbol)
        
        if len(need_to_get) > 0:
            print('Need to download new data..')
            
            new_data = self.get_new_data(need_to_get, start, end)
            for symbol in need_to_get:
                print (symbol)
                data_series = new_data[symbol].reindex(dates)
                data[symbol] = data_series
                self.cache[symbol] = data_series
            self.save()
        else:
            print ('All data cached')
        
        if dropna:data = data.dropna()
        return data
    
    def get_new_data(self, items, start, end):
        raise Exception('Must be implemented in overloaded class')