import yfinance as yf
import pandas as pd
from data_storage import GenericSeriesStorage
from datetime import datetime, timedelta
import math

class StockPrice(GenericSeriesStorage):
    
    def get_new_data(self, items, start, end):
        return yf.download(items, start = start, end = end)['Adj Close']
    
    def get_delta_increase(self, items, start, end, delta, shift=True):
        '''
        For this date return the percent change delta days in the future.
        '''
        # end needs delta added to it
        start = datetime.strptime(start, "%Y-%m-%d")
        end = datetime.strptime(end, "%Y-%m-%d")
        rang = pd.date_range(pd.Timestamp(start), pd.Timestamp(end))
        prices = self.get_stock_price(items, start, end + timedelta(delta + math.ceil(delta / 7) * 3))
        prices = prices.pct_change(delta).reindex(rang).dropna() if not shift else prices.pct_change(delta).shift(-1 * delta).reindex(rang).dropna()
        return prices


if __name__ ==  '__main__':
    from matplotlib import pyplot as plt
    sp = StockPrice()
    daily_change = sp.get_delta_increase(['MSFT', 'INTC'], "2017-01-01", "2017-04-28", 1)
    print (daily_change)
    daily_change.plot()
    plt.legend(loc='center left', bbox_to_anchor=(1.0, 0.5))

    #print (sp.get_delta_increase(['MSFT', 'INTC'], "2017-01-02", "2017-04-29", 1))
    plt.show()