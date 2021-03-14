




class data_puller():
    def __init__(self, pullers, db):
        cur_data_df = db.get_data()
        for puller in pullers:
            # get matrix of data 
            data = puller.pull_data()
            db.update(data)
        
        self.data = db.get_data()
        
        