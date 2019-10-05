# 出版社数据
import pymongo
import re
import json
import csv
import jieba
import jieba.analyse


class publish:
    dbClient = pymongo.MongoClient('mongodb://localhost:27017/')
    db = dbClient['bookdb']
    book_col = db['book']
    book_list = []
    pub_set = set()

    # 图书列表数据、出版社 名集合
    def get_book_list(self):
        all = self.book_col.find()
        for book in all:
            if 'pub_name' in book:
                self.book_list.append(book)
                self.pub_set.add(book['pub_name'])

    # 频次靠前的出版社
    def get_top_pub(self):
        self.get_book_list()
        pub_dict = dict()
        for pub in self.pub_set:
            num = 0
            for book in self.book_list:
                if pub == book['pub_name']:
                    num += 1
            pub_dict[pub] = num
        # print(pub_dict)
        data_list = self.sort_by_value(pub_dict)
        # print(len(self.pub_set))
        for i in range(0, 100):
            print(data_list[i][0], data_list[i][1])
        # print(res)

    # 字典转列表后排序
    def sort_by_value(self, d):
        l_data = list(d.items())
        l_data.sort(key=lambda x: x[1], reverse=True)
        return l_data

    def pub_num_by_year(self):

if __name__ == "__main__":
    pub = publish()
    pub.get_top_pub()
