import pymongo


class dataToMongo:
    dbClient = pymongo.MongoClient('mongodb://localhost:27017/')
    db = dbClient['bookdb']
    bookTable = db['book']

    def saveData(self, dict):
        one = self.findById(dict['_id'])
        if not one:
            self.bookTable.insert_one(dict)
        else:
            print('saved')

    def findById(self, id):
        return self.bookTable.find_one({'_id': id})

    def getAll(self):
        return self.bookTable.find()

    def deleteById(self, id):
        self.bookTable.delete_one({'_id': id})
