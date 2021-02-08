




class data_puller():
    def __init__(self, pullers, db):
        for puller in pullers:
            data = puller.pull_data()
            db.update(data)
        
        self.data = db.get_data()
        
            