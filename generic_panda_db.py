import pandas as pd
import numpy as np
import sqlite3


class generic_panda_db():
    FILENAME = 'generic.db'
    BASE_TABLE_NAME = 'basedata'
    # Columns that must be unique in a dataframe
    KEY_COLUMNS = ('Col0', 'Col1')
    verbose = False
    
    def __verbose__(self):
        return generic_panda_db.verbose
    
    def __get_db_filename__(self):
        return generic_panda_db.FILENAME
    
    def __get_key_columns__(self):
        return generic_panda_db.KEY_COLUMNS
    
    def __initialize__(self):
        self.base_data = pd.DataFrame({}, columns=self.__get_key_columns__())
        self.__set_keys__()
    
    def load_base_data(self):
        with sqlite3.connect(self.__get_db_filename__())  as con:
            try:
                # read data from SQL to pandas dataframe. 
                self.base_data = pd.read_sql_query('Select * from %s;' % generic_panda_db.BASE_TABLE_NAME, con) 
                self.__set_keys__()
                #print (self.base_data.reset_index())
                # show top 5 rows 
                print ('Table found')
                
            except pd.io.sql.DatabaseError as e:
                print ('Caught does not exist error')
                if 'no such table' in e.args[0]:
                    # table does not yet exist.  Need to make a new dataframe
                    self.__initialize__()
                    print ('No data found yet')
                else:
                    raise e
    
    def save_base_data(self):
        with sqlite3.connect(self.__get_db_filename__())  as con:
            self.base_data.to_sql(generic_panda_db.BASE_TABLE_NAME, con, if_exists='replace')
        
    def __set_keys__(self, other=None):
        if other is None:
            if self.base_data.index.names != list(self.__get_key_columns__()):
                self.base_data = self.base_data.set_index(list(self.__get_key_columns__()))        
        else:
            if other.index.names != self.__get_key_columns__():
                other.set_index(list(self.__get_key_columns__()), inplace=True)   
    
    def __init__(self):
        # create a connection 
        self.load_base_data()
        if self.__verbose__():
            print (self.base_data)
    
    # data must be a new dataframe
    def insert_data(self, data):
        if data.index.names != self.base_data.index.names:
            for col in self.__get_key_columns__():
                if col not in data.columns:
                    raise Exception("key column %s not found in input data column %s" % (col, data.columns)) 
        # Get all rows that currently exist in the data frame and set their data to the new data
        # add new columns to base data
        self.__set_keys__(data)
        if len (self.base_data) == 0:
            self.base_data = data
        else:
            new_cols = list(set(data.columns) - set(self.base_data.columns))
            if self.__verbose__():
                print ('New cols ' + str(new_cols))
            # add new rows with nan
            new_rows = list(set(data.index)-set(self.base_data.index))
            if self.__verbose__():
                print ('New rows ' + str(new_rows))
            self.base_data = pd.concat([self.base_data, pd.DataFrame(index = new_rows,columns=new_cols)], sort=False)    
            self.base_data.update(data)
        
        if self.__verbose__():
            print (self.base_data)
        self.save_base_data()
        # add the rest of the data
    

        
    def delete_data(self):
        with sqlite3.connect(self.__get_db_filename__())  as con:
            con.execute('DROP table IF EXISTS %s;' % self.BASE_TABLE_NAME)
        self.__initialize__()

    def get_data(self):
        return self.base_data.reset_index()
    
    def unit_test(self): 
        np.random.seed(1)
        self.delete_data()
        self.insert_data(pd.DataFrame({'Col0':[0,1], 'Col1':[10,11], 'Col5': [50,51], 'Col6':[60, 61]}))
        self.insert_data(pd.DataFrame({'Col0':[0,2], 'Col1':[10,31], 'Col4': [22,24]}))
        self.insert_data(pd.DataFrame({'Col0':[1], 'Col1':[11], 'Col4': [23]}))
        self.insert_data(pd.DataFrame({'Col0':[2], 'Col1':[31], 'Col5': [52], 'Col6':[62]}))
        try:
            self.insert_data(pd.DataFrame({'Col0':[2], 'Col5': [52], 'Col6':[62]}))
            raise Exception('Did not throw exception when key was missing')
        except:
            pass
        npa = self.base_data.reset_index().to_numpy()
        print ('Test array:')
        print(npa)
        expect =      np.array([[0,    10,    50.0,  60.0,   22], \
                                [1,    11,    51.0,  61.0,   23], \
                                [2,    31,    52.0,  62.0,   24]])
        print ('Expected:')
        print (expect)
        assert (expect == npa).all()
        print ('Tests passed')
        
if __name__ == '__main__':  
    gpd = generic_panda_db()
    gpd.unit_test()