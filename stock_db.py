from generic_panda_db import generic_panda_db
import stock_price


class stock_db(generic_panda_db):
    FILENAME = 'stock_market_data.db'
    # Columns that must be unique in a dataframe
    KEY_COLUMNS = ('Date', 'Symbol')
    VERBOSE = False
    
    def _print(self, msg):
        if self.__verbose__():
            print (msg)

    def __verbose__(self):
        return stock_db.VERBOSE
    
    def __get_db_filename__(self):
        return stock_db.FILENAME
    
    def __get_key_columns__(self):
        return stock_db.KEY_COLUMNS
    
    def __init__(self):
        super().__init__()


if __name__ == '__main__':  
        stock_db()