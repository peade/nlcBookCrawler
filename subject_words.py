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
    def fill_a_col(self):
        all = self.book_col.find()
        # year_list = ['2000', '2001', '2002', '2003', '2004', '2005', '2006', '2007', '2008', '2009', '2010', '2011',
        #              '2012', '2013', '2014', '2015', '2016', '2017', '2018']
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
                # print(book['subject_terms'])
                # print(result)
                book['a_list'] = result
                book_list.append(book)
                temp_set = set(result)
                word_set = word_set.union(temp_set)
        # 构建主题词键值对，每一个主题词下面，有各个年份的初始数量为0
        subject_dict = dict()
        for sub in word_set:
            temp_dict = {'word': sub, 'total': 0}
            for year in self.year_array:
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
        # print(data_list)
        # for da in data_list:
        #     print(da)
        # 保存到数据库
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

    # 主要款目 频次排行
    def a_top_frequency(self):
        all = self.sub_a_col.find().sort('total', pymongo.DESCENDING).limit(100)
        for word in all:
            print(word['word'], word['total'])
        print(all.count())

    # 提取数据
    def subject_a_total_num(self):
        all = self.sub_a_col.find({'2000': {'$gt': 0}})
        all.sort('2000', pymongo.DESCENDING)
        for word in all:
            print(word['word'], word['2000'])
        print(all.count())

    # 主标目 按年数量
    def a_year_num(self):
        for year in self.year_array:
            all = self.sub_a_col.find({year: {'$gt': 0}}).sort(year, pymongo.DESCENDING).limit(10)
            print(year, all.count())

            # for word in all:
            #     print(word['word'], word[year])

    # 主要标目，各阶段主题词排行榜
    def a_gap_high(self):
        set_data = set()
        dict_data = dict()
        for i in range(1, 5):
            gap_data = self.sub_a_col.find({'gap' + str(i): {'$gt': 0}}).limit(20).sort('gap' + str(i),
                                                                                        pymongo.DESCENDING)
            print('gap' + str(i), gap_data.count())
            print(self.gap_array[i - 1], '频次')
            dict_data[self.gap_array[i - 1]] = []
            for item in gap_data:
                print(item['word'], item['gap' + str(i)])
                set_data.add(item['word'])
                dict_data[self.gap_array[i - 1]].append(item['word'])
        for word in set_data:
            bo = True
            for key in dict_data:
                if word not in dict_data[key]:
                    bo = False
            if bo:
                print(word)

    # 主要标目，各个阶段 新数据
    def a_gap_new(self):
        gap2_data = self.sub_a_col.find({'gap2': {'$gt': 0}, 'gap1': 0}).sort('gap2', pymongo.DESCENDING).limit(10)
        print("阶段2>阶段1", "频次")
        for item in gap2_data:
            print(item['word'], item['gap2'])
        gap3_data = self.sub_a_col.find({'gap3': {'$gt': 0}, 'gap2': 0}).sort('gap3', pymongo.DESCENDING).limit(10)
        print('阶段3>阶段2', "频次")
        for gap3 in gap3_data:
            print(gap3['word'], gap3['gap3'])

        gap4_data = self.sub_a_col.find({'gap4': {'$gt': 0}, 'gap3': 0}).sort('gap4', pymongo.DESCENDING).limit(10)
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
        print('clean 全主题')

    # 全主题词 获取所有
    def full_all(self):
        all = self.sub_col.find()
        print(all.count())
        for word in all:
            print(word['word'])

    # 全主题词 表格数据填充
    def full_fill_col(self):
        all = self.book_col.find()
        # year_list = ['2000', '2001', '2002', '2003', '2004', '2005', '2006', '2007', '2008', '2009', '2010', '2011',
        #              '2012', '2013', '2014', '2015', '2016', '2017', '2018']
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
                # print(book['subject_terms'])
                # print(book['a_list'])
                # print(book['sub_words'])
        # print(word_set)
        # 构建主题词键值对，每一个主题词下面，有各个年份的初始数量为0
        subject_dict = dict()
        for sub in word_set:
            temp_dict = {'word': sub, 'total': 0}
            for year in self.year_array:
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

            for da in data_list:
                print(da)
            data_list.append(subject_dict[sub_item])
        # print(subject_dict)
        # print(data_list)

        # 保持数据到数据库
        self.sub_col.insert_many(data_list)

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
            print(self.gap_array[i - 1], gap_data.count())
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
            # if bo:
            #     print(word)

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
                if similar >= 1:
                    similar = 1
                if similar < 0.0001:
                    similar = 0
                if similar > 0:
                    similar = round(similar, 4)
                word_similary[word].append(similar)
        print('------------------------频率矩阵---------------------')
        for key in word_frequency:
            freq_str = ' '.join('%s' % id for id in word_frequency[key])
            print(key, freq_str)
        print('------------------------相似矩阵---------------------')
        for key in word_similary:
            simi_str = ' '.join('%s' % id for id in word_similary[key])
            print(key, simi_str)
        print('-------------------------相异矩阵---------------------')
        for key in word_similary:
            list = word_similary[key]
            new_list = []
            for num in list:
                new_num = 1 - num
                if new_num >= 1:
                    new_num = 1
                if new_num < 1:
                    new_num = round(new_num, 4)
                if new_num <= 0:
                    new_num = 0
                new_list.append(new_num)
            disi_str = ' '.join('%s' % id for id in new_list)
            print(key, disi_str)


if __name__ == "__main__":
    subject = subject_process()

    subject.full_gap_matrix()
