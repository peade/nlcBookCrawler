import pymongo
import re


class dataToMongo:
    dbClient = pymongo.MongoClient('mongodb://localhost:27017/')
    db = dbClient['bookdb']
    bookTable = db['book']
    backBookTable = db['bookBackUp']

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

    def findOne(self):
        item = self.bookTable.find_one()
        print(item)

    def findByPublish(self, word):
        item = self.bookTable.find({'publisher': re.compile(word)})
        for it in item:
            print(it['_id'])
            # self.deleteById(it['_id'])
        print(item.count())

    def backDB(self):
        print('back')
        allBook = self.bookTable.find()
        all = self.backBookTable.find()
        print(all.count(), allBook.count())
        if all.count() == 0:
            self.backBookTable.insert_many(allBook)


class handleBookInfo:
    dbClient = pymongo.MongoClient('mongodb://localhost:27017/')
    db = dbClient['bookdb']
    bookTable = db['book']

    def find_one(self):
        book = self.bookTable.find_one()
        # print(book)
        return book

    def reg_isbn(self, str):
        # str = '|a 7-5333-0855-7 |d CNY18.00'
        pattern = r'^\|a\s(.*)\s\|d\s(CNY.*)$'
        result = re.match(pattern, str)
        print(result.group(1), result.group(2))
        return {
            'isbn_no': result.group(1),
            'price': result.group(2)
        }

    def reg_book_name(self, str):
        pattern = r'\|a\s*([^\s]*)\s*'
        result = re.search(pattern, str)
        return result.group(1)

    def reg_pub_info(self, str):
        city_pattern = r'\|a\s*([^\s]*)\s*'
        name_pattern = r'\|c\s*([^\s]*)\s*'
        year_pattern = r'\|d\s*([^\s]*)\s*'

        city_result = re.search(city_pattern, str)
        name_result = re.search(name_pattern, str)
        year_result = re.search(year_pattern, str)
        return {
            'pub_city': city_result.group(1),
            'pub_name': name_result.group(1),
            'pub_year': year_result.group(1)
        }

    def update_one(self, query, newVal):
        self.bookTable.update_one(query, newVal)

    def find_limit(self, num, query=None):
        if query is None:
            query = {}
        return self.bookTable.find(query).limit(num)

    def find_duplicat(self):
        pipeline = [
            {'$project': {
                'isbn': 1,
                'name': 1,
                'publisher': 1
            }},
            {'$group': {'_id': {
                'isbn': '$isbn',
                'name': '$name',
                'publisher': '$publisher'
            }, 'type_count': {'$sum': 1}}},
            {'$sort': {'name': 1}}
        ]
        res = self.bookTable.aggregate(pipeline)
        for r in res:
            print(r)


if __name__ == "__main__":
    # dbOp = dataToMongo()
    # # dbOp.findByPublish("全国图书馆文献缩微中心")
    # dbOp.backDB()
    handle = handleBookInfo()
    item = handle.find_one()
    handle.find_duplicat()
    # handle.regBookName(item['name'])
    # pub_info = handle.reg_pub_info(item['publisher'])
    # books = handle.find_limit(100)
    # print(books.count())
    # for book in books:
    #     print(book)
    # dic = handle.reg_isbn(item['isbn'])
    # item['isbnNo'] = dic['isbnNo']
    # item['price'] = dic['price']
    # handle.updateOne({'_id': item['_id']}, {'$unset': {'isbnNo': dic['isbnNo']}})
    print(item)
