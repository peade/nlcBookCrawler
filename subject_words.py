import pymongo
import re
import json
import csv
import jieba
import jieba.analyse


class subject_process:
    dbClient = pymongo.MongoClient('mongodb://localhost:27017/')
    db = dbClient['bookdb']
    book_col = db['book']
    sub_col = db['subject']
    sub_a_col = db['subA']

    def get_books(self):
        all = self.book_col.find().limit(10)
        for book in all:
            print(book)

    def get_sub_data(self):
        all = self.sub_col.find().limit(10)
        for sub in all:
            print(sub)

    # 获取6字段条数
    def count_6_num(self):
        all = self.book_col.find()
        one = 0
        two = 0
        three = 0
        four = 0
        more = 0
        for book in all:
            terms = book['subject_terms']
            pattern = r'\$\$'
            res = re.findall(pattern, terms)
            # print(res)
            if len(res) == 1:
                one += 1
            elif len(res) == 2:
                two += 1
            elif len(res) == 3:
                three += 1
            elif len(res) == 4:
                four += 1
            else:
                print(len(res))
                more += 1
        print(one, two, three, four, more)

    # 获取主要款目
    def subject_a_handle(self):
        all = self.book_col.find()
        year_list = ['2000', '2001', '2002', '2003', '2004', '2005', '2006', '2007', '2008', '2009', '2010', '2011',
                     '2012', '2013', '2014', '2015', '2016', '2017', '2018']
        # 主要款目集合
        word_set = set()
        # 图书数据数组 从数据库里查出来的值，只能进行一次遍历
        book_list = []
        for book in all:
            if 'subject_terms' in book:
                # term_text = book['subject_terms'].replace('$$', '')
                term_text = book['subject_terms']
                pattern = r'\|a\s([^\|\s\$]+)[\$|\s|$]'
                result = re.findall(pattern, string=term_text)
                book['a_list'] = result
                book_list.append(book)
                temp_set = set(result)
                word_set = word_set.union(temp_set)
        # 构建主题词键值对，每一个主题词下面，有各个年份的初始数量为0
        subject_dict = dict()
        for sub in word_set:
            temp_dict = {'word': sub, 'total': 0}
            for year in year_list:
                temp_dict[year] = 0
            subject_dict[sub] = temp_dict
        for book in book_list:
            for word in book['a_list']:
                subject_dict[word][book['pub_year']] += 1
                subject_dict[word]['total'] += 1
        # 将数据批量存进数据库
        data_list = []
        for sub_item in subject_dict:
            data_list.append(subject_dict[sub_item])
        self.save_subject_a(data_list)

    # 保存主要款目数据
    def save_subject_a(self, data_list):
        self.sub_a_col.insert_many(data_list)

    # 清空主要宽目数据
    def clean_sub_a_col(self):
        self.sub_a_col.remove({})

    # 主要款目数据
    def subject_a_data(self):
        all = self.sub_a_col.find()
        for item in all:
            print(item)
        print(all.count())

    # 提取数据
    def subject_a_total_num(self):
        all = self.sub_a_col.find({'2000': {'$gt': 0}})
        all.sort('2000', pymongo.DESCENDING)
        for word in all:
            print(word['word'], word['2000'])
        print(all.count())

    # 按年提取数据
    def year_sub_num(self, year):
        all = self.sub_a_col.find({year: {'$gt': 0}}).sort(year, pymongo.DESCENDING).limit(10)
        print(year, all.count())
        for word in all:
            print(word['word'], word[year])

    def each_year_number(self):
        year_list = ['2000', '2001', '2002', '2003', '2004', '2005', '2006', '2007', '2008', '2009', '2010', '2011',
                     '2012', '2013', '2014', '2015', '2016', '2017', '2018']
        for year in year_list:
            self.year_sub_num(year)

    # 5年一阶段数量
    def gap_year_num(self):
        all = self.sub_a_col.find()
        for book in all:
            gap_data = dict()
            gap_data['gap1'] = book['2000'] + book['2001'] + book['2002'] + book['2003'] + book['2004']
            gap_data['gap2'] = book['2005'] + book['2006'] + book['2007'] + book['2008'] + book['2009']
            gap_data['gap3'] = book['2010'] + book['2011'] + book['2012'] + book['2013'] + book['2014']
            gap_data['gap4'] = book['2015'] + book['2016'] + book['2017'] + book['2018']
            self.sub_a_col.update_one(book, {'$set': gap_data})


if __name__ == "__main__":
    subject = subject_process()
    # 存主款目到数据库
    # subject.subject_a_handle()
    # 清理主款目数据表数据
    # subject.clean_sub_a_col()

    # 获取主款目数据
    # subject.subject_a_total_num()

    # 按年数据
    subject.gap_year_num()
