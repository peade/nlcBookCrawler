# 出版社数据
import pymongo
import re
import json
import csv
import jieba
import jieba.analyse


class author:
    dbClient = pymongo.MongoClient('mongodb://localhost:27017/')
    db = dbClient['bookdb']
    book_col = db['book']
    book_list = []
    author_set = set()

    # 图书列表数据、出版社 名集合
    def get_book_list(self):
        all = self.book_col.find()
        for book in all:
            # print(book['name'])
            # print(book['author'])
            self.book_list.append(book)
        # print(self.book_list)

    # 获取作者姓名，频次
    def get_author_name(self):
        self.get_book_list()
        au_set = set()
        au_dict = dict()
        book_au_list = []
        for book in self.book_list:
            if 'author' in book:
                pattern = r'\|a\s+([^\$\|]+)[\s\|$]?'
                result = re.findall(pattern, string=book['author'])
                book_au_list.append(result)
                temp_set = set(result)
                au_set = au_set.union(temp_set)
        for au in au_set:
            num = 0
            for book in book_au_list:
                if au in book:
                    num += 1
            au_dict[au] = num
        for au in au_dict:
            print(au, au_dict[au])
        print(len(au_dict))

    # 责任方式 类型
    def get_author_duty(self):
        self.get_book_list()
        duty_set = set()
        duty_dict = dict()
        duty_list = []
        for book in self.book_list:
            pattern = r'\|4\s([^\s\|\$]+)'
            result = re.findall(pattern, book['author'])
            res_list = []
            for word in result:
                temp_list = word.split('/')
                res_list.extend(temp_list)
            duty_list.append(result)
            temp_set = set(result)
            duty_set = duty_set.union(temp_set)
        for duty in duty_set:
            num = 0
            for item in duty_list:
                for word in item:
                    if word == duty:
                        num += 1
            duty_dict[duty] = num
        for duty in duty_dict:
            print(duty, duty_dict[duty])
        print(len(duty_set))

    # 根据作者查书
    def get_book_by_author(self, name):
        all = self.book_col.find({'author': {"$regex": name}})
        for book in all:
            print(book)

    # 获取作者国籍
    def get_book_nation(self):
        self.get_book_list()
        nation_set = set()
        na_list = []
        na_dict = dict()
        for book in self.book_list:
            pattern = r'\|f\s\(([^)]+)\)'
            res = re.findall(pattern, book['name'])
            if len(res) > 0:
                na_str = res[0].strip()
                if na_str:
                    na_list.append(na_str)
                    nation_set.add(na_str)
        for na in nation_set:
            num = 0
            for word in na_list:
                if word == na:
                    num += 1
            na_dict[na] = num
        for na in na_dict:
            print(na, na_dict[na])

    # 字典转列表后排序
    def sort_by_value(self, d):
        l_data = list(d.items())
        l_data.sort(key=lambda x: x[1], reverse=True)
        return l_data


if __name__ == "__main__":
    au = author()
    # au.get_book_by_author('程焕文')
    au.get_book_nation()
