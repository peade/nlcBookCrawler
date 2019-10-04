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
    year_array = ['2000', '2001', '2002', '2003', '2004', '2005', '2006', '2007', '2008', '2009', '2010', '2011',
                  '2012', '2013', '2014', '2015', '2016', '2017', '2018']
    gap_array = ['2000-2004', '2005-2009', '2010-2014', '2015-2018']

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
                pattern = r'\|a\s([^\|\s\$]+)[\$|\s|$]?'
                result = re.findall(pattern, string=term_text)
                print(book['subject_terms'])
                print(result)
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
        # self.save_subject_a(data_list)

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

    # 主要标目，各阶段主题词排行榜
    def get_a_gap_data(self):
        top20 = set()
        gap1 = self.sub_a_col.find({'gap1': {'$gt': 0}}).sort('gap1', pymongo.DESCENDING).limit(20)
        print('2000-2004', gap1.count())
        list_gap1 = []
        for g1 in gap1:
            print(g1['word'], g1['gap1'])
            top20.add(g1['word'])
            list_gap1.append(g1['word'])
        gap2 = self.sub_a_col.find({'gap2': {'$gt': 0}}).sort('gap2', pymongo.DESCENDING).limit(20)
        print('2005-2009', gap2.count())
        list_gap2 = []
        for g2 in gap2:
            print(g2['word'], g2['gap2'])
            top20.add(g2['word'])
            list_gap2.append(g2['word'])
        gap3 = self.sub_a_col.find({'gap3': {'$gt': 0}}).sort('gap3', pymongo.DESCENDING).limit(20)
        print('2010-2014', gap3.count())
        list_gap3 = []
        for g3 in gap3:
            print(g3['word'], g3['gap3'])
            top20.add(g3['word'])
            list_gap3.append(g3['word'])
        gap4 = self.sub_a_col.find({'gap4': {'$gt': 0}}).sort('gap4', pymongo.DESCENDING).limit(20)
        print('2015-2018', gap1.count())
        list_gap4 = []
        for g4 in gap4:
            print(g4['word'], g4['gap4'])
            top20.add(g4['word'])
            list_gap4.append(g4['word'])

        print('2000-2004', gap1.count())
        print('2005-2009', gap2.count())
        print('2010-2014', gap3.count())
        print('2015-2018', gap1.count())

        for word in top20:
            if word in list_gap1 and word in list_gap2 and word in list_gap3 and word in list_gap4:
                print(word)

    # 主要标目，各个阶段 新数据
    def get_gap_new_data(self):
        gap2_data = self.sub_a_col.find({'gap2': {'$gt': 0}, 'gap1': 0}).sort('gap2', pymongo.DESCENDING).limit(10)
        print("阶段2>阶段1", "频次")
        for item in gap2_data:
            print(item['word'], item['gap2'])
        gap3_data = self.sub_a_col.find({'gap3': {'$gt': 0}, 'gap2': 0, 'gap1': 0}) \
            .sort('gap3',
                  pymongo.DESCENDING).limit(10)
        print('阶段3>阶段2', "频次")
        for gap3 in gap3_data:
            print(gap3['word'], gap3['gap3'])

        gap4_data = self.sub_a_col.find({'gap4': {'$gt': 0}, 'gap3': 0, 'gap2': 0, 'gap1': 0}) \
            .sort('gap4',
                  pymongo.DESCENDING).limit(
            10)
        print('阶段4>阶段3', "频次")
        for gap4 in gap4_data:
            # print(gap4)
            print(gap4['word'], gap4['gap4'])
        print('gap2', gap2_data.count())
        print('gap3', gap3_data.count())
        print('gap4', gap4_data.count())

    # 清空全主题词表
    def clean_full_col(self):
        self.sub_col.remove({})
        print('clean')

    # 全主题词 获取所有
    def full_all(self):
        all = self.sub_col.find()
        print(all.count())
        for word in all:
            print(word['word'])

    # 全主题词 表格数据填充
    def full_fill_col(self):
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
                # print(term_text)
                # print(book['sub_words'])
                pattern = r'\|[a-z]\s([^\|\s\$]+)[\$\s$]?'
                # pattern = r'\|[a-z]\s+([^\s\|\$]*)'
                result = re.findall(pattern, string=term_text)
                book['a_list'] = set(result)
                book_list.append(book)
                temp_set = set(result)
                word_set = word_set.union(temp_set)
                print(book['subject_terms'])
                print(book['a_list'])
                print(book['sub_words'])
        # print(word_set)
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
            subject_dict[sub_item]['gap1'] = subject_dict[sub_item]['2000'] + subject_dict[sub_item]['2001'] + \
                                             subject_dict[sub_item]['2002'] + subject_dict[sub_item]['2003'] + \
                                             subject_dict[sub_item]['2004']
            subject_dict[sub_item]['gap2'] = subject_dict[sub_item]['2005'] + subject_dict[sub_item]['2006'] + \
                                             subject_dict[sub_item]['2007'] + subject_dict[sub_item]['2008'] + \
                                             subject_dict[sub_item]['2009']
            subject_dict[sub_item]['gap3'] = subject_dict[sub_item]['2010'] + subject_dict[sub_item]['2011'] + \
                                             subject_dict[sub_item]['2012'] + subject_dict[sub_item]['2013'] + \
                                             subject_dict[sub_item]['2014']
            subject_dict[sub_item]['gap4'] = subject_dict[sub_item]['2015'] + subject_dict[sub_item]['2016'] + \
                                             subject_dict[sub_item]['2017'] + subject_dict[sub_item]['2018']

            data_list.append(subject_dict[sub_item])
        # print(subject_dict)
        # print(data_list)

        # 保持数据到数据库
        # self.sub_col.insert_many(data_list)

    # 全主题词 高频主题词
    def full_high(self):
        all = self.sub_col.find().sort('total', pymongo.DESCENDING).limit(100)
        for word in all:
            print(word['word'], word['total'])
        print(all.count())

    # 全主题词 每年数量
    def full_each_year(self):
        for year in self.year_array:
            all = self.sub_col.find({year: {'$gt': 0}}).sort(year, pymongo.DESCENDING).limit(10)
            print(year, all.count())
            # for word in all:
            #     print(word['word'], word[year])

    # 全主题词 各个阶段高频主题词
    def full_gap_high(self):
        set_data = set()
        dict_data = dict()
        for i in range(1, 5):
            gap_data = self.sub_col.find({'gap' + str(i): {'$gt': 0}}).limit(20).sort('gap' + str(i),
                                                                                      pymongo.DESCENDING)
            # print(self.gap_array[i - 1], '频次')
            dict_data[self.gap_array[i - 1]] = []
            for item in gap_data:
                # print(item['word'], item['gap' + str(i)])
                set_data.add(item['word'])
                dict_data[self.gap_array[i - 1]].append(item['word'])
        for word in set_data:
            bo = True
            for key in dict_data:
                if word not in dict_data[key]:
                    bo = False
            if bo:
                print(word)

    # 全主题词 各阶段新增主题词
    def full_gap_new(self):
        gap2_data = self.sub_col.find({'gap2': {'$gt': 0}, 'gap1': 0}).sort('gap2', pymongo.DESCENDING).limit(10)
        print("阶段2>阶段1", "频次")
        for item in gap2_data:
            print(item['word'], item['gap2'])
        gap3_data = self.sub_col.find({'gap3': {'$gt': 0}, 'gap2': 0, 'gap1': 0}) \
            .sort('gap3',
                  pymongo.DESCENDING).limit(10)
        print('阶段3>阶段2', "频次")
        for gap3 in gap3_data:
            print(gap3['word'], gap3['gap3'])

        gap4_data = self.sub_col.find({'gap4': {'$gt': 0}, 'gap3': 0, 'gap2': 0, 'gap1': 0}) \
            .sort('gap4',
                  pymongo.DESCENDING).limit(
            10)
        print('阶段4>阶段3', "频次")
        for gap4 in gap4_data:
            # print(gap4)
            print(gap4['word'], gap4['gap4'])
        print('gap2', gap2_data.count())
        print('gap3', gap3_data.count())
        print('gap4', gap4_data.count())

    # 全主题词 矩阵
    def full_gap_matrix(self):
        all_word = self.sub_col.find().sort('total', pymongo.DESCENDING).limit(100)
        book_data = self.book_col.find()
        all_book = []
        for book in book_data:
            all_book.append(book)
        top100_word = dict()
        word100 = []
        word_frequency = dict()
        word_similary = dict()
        for word in all_word:
            top100_word[word['word']] = word['total']
            word100.append(word['word'])
        for word in word100:
            word_frequency[word] = []
            word_similary[word] = []
            for w in word100:
                print(word, w)
                num = 0
                for book in all_book:
                    arr = book['sub_words'].split('|')
                    if word in arr and w in arr:
                        num += 1
                        print(arr)
                word_frequency[word].append(num)
                print(top100_word[word], top100_word[w])
                print(num, top100_word[word], top100_word[w])
                similar = num * num / (top100_word[word] * top100_word[w])
                if similar < 0.0001:
                    similar = 0
                if similar > 0:
                    similar = round(similar, 4)
                word_similary[word].append(similar)
        # print('------------------------频率矩阵---------------------')
        # for key in word_frequency:
        #     print(key, word_frequency[key])
        # print('------------------------相似矩阵---------------------')
        # for key in word_similary:
        #     print(key, word_similary[key])
        # print('-------------------------相异矩阵---------------------')
        # for key in word_similary:
        #     list = word_similary[key]
        #     new_list = []
        #     for num in list:
        #         new_num = 1 - num
        #         if new_num >= 1:
        #             new_num = 1
        #         if new_num < 1:
        #             new_num = round(new_num, 4)
        #         new_list.append(new_num)
        #     print(key, new_list)


if __name__ == "__main__":
    subject = subject_process()

    subject.subject_a_handle()
