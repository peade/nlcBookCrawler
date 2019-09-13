import pymongo
import re


class analysis:
    db_client = pymongo.MongoClient('mongodb://localhost:27017/')
    db = db_client['bookdb']
    book_table = db['book']

    def __init__(self):
        self.pn = 1

    def get_data_num(self):
        all = self.book_table.find().limit(100)
        print(all.count())

    def get_pub_info(self):
        page_size = 10
        page_no = 1
        total = None
        city_data = set()
        while True:
            skip = page_size * (page_no - 1)
            if total is not None and skip >= total:
                break
            page_record = self.book_table.find({}).limit(page_size).skip(skip)
            total = page_record.count()
            page_no += 1
            for item in page_record:
                if 'pub_city' in item:
                    city_data.add(item['pub_city'])
        print(city_data)

    def get_year_num(self):
        pipeline = [
            {'$project': {
                'pub_year': 1
            }},
            {'$group': {'_id': {
                'year': '$pub_year'
            }, 'type_count': {'$sum': 1}}},
            {'$sort': {'_id': -1}}
        ]
        res = self.book_table.aggregate(pipeline)
        for r in res:
            print(r['_id']['year'], r['type_count'])

    def clean_pub_year(self):
        all = self.book_table.find()
        for item in all:
            # 清理没有出版年份的数据
            if int(item['pub_year']) < 2000:
                print(item['pub_year'])
                # self.book_table.delete_one({'_id': item['_id']})
            # if 'pub_year' not in item:
            #     print(int(item['pub_year']) < 2000)
            # self.book_table.delete_one({'_id': item['_id']})
            # reg = r'\[|\]|-|\.\d+'
            # if 'pub_year' in item:
            #     pub_year = re.sub(reg, '', item['pub_year'])
            #     query = {'_id': item['_id']}
            #     value = {'$set': {'pub_year': pub_year}}
            #     self.book_table.update_one(query, value)
            # result = re.search(reg, item['pub_year'])

    def back_data(self, name=''):
        print(name)
        list = self.db.list_collection_names()
        print(list)
        if name:
            if name not in list:
                new_col = self.db[name]
                all_book = self.book_table.find()
                new_col.insert_many(all_book)
            else:
                book_back = self.db[name]
                print(book_back.find().count())

    def clean_pub_city(self):
        page_size = 10
        page_no = 1
        total = None
        while True:
            skip = page_size * (page_no - 1)
            if total is not None and skip >= total:
                break
            page_record = self.book_table.find({}).limit(page_size).skip(skip)
            total = page_record.count()
            page_no += 1
            for item in page_record:
                if 'pub_city' in item:
                    reg = r'\[|\]'
                    city_str = re.sub(reg, '', item['pub_city'])
                    query = {'_id': item['_id']}
                    value = {'$set': {'pub_city': city_str}}
                    self.book_table.update_one(query, value)

    def delete_not_mainland(self):
        city_list = ['澳门', '出版地不详', '高雄', '高雄县', '嘉义县', '金门县', '马来西亚', '美国', '南投',
                     '南投县', '澎湖', '澎湖县', '屏东县', '日内瓦', '台北', '台北县', '台南', '台南县', '台中',
                     '台中县', '新北', '新加坡', '宜兰县', '香港']
        for city in city_list:
            query = {'pub_city': city}
            self.book_table.delete_many(query)
        # page_size = 10
        # page_no = 1
        # total = None
        # while True:
        #     skip = page_size * (page_no - 1)
        #     if total is not None and skip >= total:
        #         break
        #     page_record = self.book_table.find({}).limit(page_size).skip(skip)
        #     total = page_record.count()
        #     page_no += 1
        #     for item in page_record:
        #         if 'pub_city' in item:
        #             if item['pub_city'] in city_list:
        #                 print(item)


if __name__ == "__main__":
    ana = analysis()
    ana.get_data_num()
