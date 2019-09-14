import pymongo
import re
import json
import csv


class dataToMongo:
    dbClient = pymongo.MongoClient('mongodb://localhost:27017/')
    db = dbClient['bookdb']
    book_table = db['book']
    backbook_table = db['bookBackUp']

    def saveData(self, dict):
        one = self.findById(dict['_id'])
        if not one:
            self.book_table.insert_one(dict)
        else:
            print('saved')

    def findById(self, id):
        return self.book_table.find_one({'_id': id})

    def getAll(self):
        return self.book_table.find()

    def deleteById(self, id):
        self.book_table.delete_one({'_id': id})

    def findOne(self):
        item = self.book_table.find_one()
        print(item)

    def findByPublish(self, word):
        item = self.book_table.find({'publisher': re.compile(word)})
        for it in item:
            print(it['_id'])
            # self.deleteById(it['_id'])
        print(item.count())

    def backDB(self):
        print('back')
        allBook = self.book_table.find()
        all = self.backbook_table.find()
        print(all.count(), allBook.count())
        if all.count() == 0:
            self.backbook_table.insert_many(allBook)


# class sub_item():
#     def __init__(self, word):
#         self.word = word
#         year_list = ['2000', '2001', '2002', '2003', '2004', '2005', '2006', '2007', '2008', '2009', '2010', '2011',
#                      '2012', '2013', '2014', '2015', '2016', '2017', '2018']
#         for year in year_list:
#             self[year] = 0
#
#     def add_num(self, year):
#         self[year] += 1


class handleBookInfo:
    dbClient = pymongo.MongoClient('mongodb://localhost:27017/')
    db = dbClient['bookdb']
    book_table = db['book']
    sub_col = db['subject']

    def find_one(self):
        book = self.book_table.find_one()
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
            self.book_table.update_one({'_id': item['_id']}, {'$set': book_dic})

    def update_one(self, query, newVal):
        self.book_table.update_one(query, {'$set': newVal})

    def find_limit(self, num, query=None):
        if query is None:
            query = {}
        return self.book_table.find(query).limit(num)

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
            {'$sort': {'type_count': -1}},
            {'$limit': 10}
        ]
        res = self.book_table.aggregate(pipeline)
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
            page_record = self.book_table.find({}).limit(page_size).skip(skip)
            total = page_record.count()
            page_no += 1
            for item in page_record:
                print(item)
                # self.item_process(item)

    def sort(self):
        result = self.book_table.find({}).limit(20).sort('name', 1)
        for r in result:
            print(r)

    def deleteById(self, id):
        self.book_table.delete_one({'_id': id})

    def export_json(self):
        data = self.book_table.find()
        list = []
        for da in data:
            list.append(da)
        with open('d:/book.json', 'w', encoding='utf8') as file:
            file.write(json.dumps(list, indent=2, ensure_ascii=False))

    def export_csv(self):
        data = self.book_table.find()
        list = []
        for da in data:
            li = []
            for d in da:
                li.append(da[d])
            list.append(li)
        with open('d:/book.csv', 'w', encoding='utf8') as file:
            writer = csv.writer(file)
            writer.writerow([
                '_id', 'isbn', 'name', 'version', 'publisher', 'pages', 'series', 'abstract', 'subject_terms',
                'author', 'price', 'bookname', 'isbn_no', 'pub_city', 'pub_name', 'pub_year'
            ])
            for l in list:
                writer.writerow(l)

    # 主题词处理
    def subject_word(self):
        all = self.book_table.find()
        for item in all:
            if 'sub_words' in item:
                print(item['sub_words'])
            elif 'subject_terms' in item:
                pattern = r'\|[a-z]\s+([^\s\|\$]*)'
                result = re.findall(pattern, item['subject_terms'])
                words = set(result)
                word_str = '|'.join(list(words))
                self.book_table.update_one({'_id': item['_id']}, {'$set': {'sub_words': word_str}})

    # 主题词按年分析
    def year_subject(self):
        year_list = ['2000', '2001', '2002', '2003', '2004', '2005', '2006', '2007', '2008', '2009', '2010', '2011',
                     '2012', '2013', '2014', '2015', '2016', '2017', '2018']
        exclude_list = ['研究', '方法', '分析', '关系', '计划', '技术', '建设',
                        '教材', '介绍', '文集', '论文集', '选集', '作品', '作品集']
        all_book = self.book_table.find()
        subject_set = set()
        book_list = []
        for book in all_book:
            book_list.append(book)
            pattern = r'([^]?[^|]+[$]?)'
            result = re.findall(pattern, book['sub_words'])
            temp_set = set(result)
            subject_set = subject_set.union(temp_set)
        subject_dict = dict()
        for sub in subject_set:
            temp_dict = {'word': sub, 'total': 0}
            for year in year_list:
                temp_dict[year] = 0
            subject_dict[sub] = temp_dict
        for book in book_list:
            pattern = r'([^]?[^|]+[$]?)'
            result = re.findall(pattern, book['sub_words'])
            for word in result:
                subject_dict[word]['total'] += 1
                subject_dict[word][book['pub_year']] += 1

        # print(subject_dict)
        data_list = []
        for sub_item in subject_dict:
            data_list.append(subject_dict[sub_item])
        # print(data_list)
        self.sub_col.insert_many(data_list)

    # 主题词 表
    def sub_operate(self):
        result = self.sub_col.find().limit(10)
        print(result.count())
        for r in result:
            print(r)


if __name__ == "__main__":
    handle = handleBookInfo()
    handle.sub_operate()
