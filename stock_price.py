import yfinance as yf
import pandas as pd
from data_storage import GenericSeriesStorage

class StockPrice(GenericSeriesStorage):
    
    def get_new_data(self, items, start, end):
        return yf.download(items, start = start, end = end)['Adj Close']
    



if __name__ ==  '__main__':
    sp = StockPrice()
    print (sp.get_stock_price(['MSFT', 'INTC'], "2017-01-01", "2017-04-30"))
    print (sp.get_stock_price(['MSFT', 'INTC'], "2017-01-02", "2017-04-29"))