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

    def item_process(self, item):
        book_dic = {}
        isbn_pattern = r'^\|a\s(.*)\s\|d\s(CNY.*)$'
        isbn_result = re.match(isbn_pattern, item['isbn'])
        if isbn_result is not None:
            book_dic['isbn_no'] = isbn_result.group(1)
            book_dic['price'] = isbn_result.group(2)

        bookname_pattern = r'\|a\s*([^\s]*)\s*'
        bookname_result = re.search(bookname_pattern, item['name'])
        if bookname_result is not None:
            book_dic['bookname'] = bookname_result.group(1)

        city_pattern = r'\|a\s*([^\s]*)\s*'
        name_pattern = r'\|c\s*([^\s]*)\s*'
        year_pattern = r'\|d\s*([^\s]*)\s*'

        city_result = re.search(city_pattern, item['publisher'])
        name_result = re.search(name_pattern, item['publisher'])
        year_result = re.search(year_pattern, item['publisher'])
        if city_result is not None:
            book_dic['pub_city'] = city_result.group(1)
        if name_result is not None:
            book_dic['pub_name'] = name_result.group(1)
        if year_result is not None:
            book_dic['pub_year'] = year_result.group(1)
        # print(book_dic)
        # update_item
        if book_dic:
            self.bookTable.update_one({'_id': item['_id']}, {'$set': book_dic})

    def update_one(self, query, newVal):
        self.bookTable.update_one(query, {'$set': newVal})

    def find_limit(self, num, query=None):
        if query is None:
            query = {}
        return self.bookTable.find(query).limit(num)

    def aggregate(self):
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

    def page_query(self):
        page_size = 10
        page_no = 1
        total = None
        while True:
            skip = page_size * (page_no - 1)
            if total is not None and skip >= total:
                break
            print(page_no)
            page_record = self.bookTable.find({}).limit(page_size).skip(skip)
            total = page_record.count()
            page_no += 1
            for item in page_record:
                print(item)
                # self.item_process(item)


if __name__ == "__main__":
    # dbOp = dataToMongo()
    # # dbOp.findByPublish("全国图书馆文献缩微中心")
    # dbOp.backDB()
    handle = handleBookInfo()
    # item = handle.find_one()
    # print(item)
    # handle.item_process(item)

    handle.page_query()
