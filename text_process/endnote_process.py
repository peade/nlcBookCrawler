import re
import pymongo
import xml.sax


class kw_to_mongo:
    def __init__(self):
        self.dbClient = pymongo.MongoClient('mongodb://localhost:27017/')
        self.db = self.dbClient['bookdb']
        self.kw_collection = self.db['key_words']

    def save_kw(self, kw):
        one = self.kw_collection.find_one({'word': kw})
        if not one:
            self.kw_collection.insert_one({'word': kw})
        else:
            print(kw)

    def get_all(self):
        all = self.kw_collection.find({})
        print(all.count())
        # return all
        for word in all:
            print(word['word'])


class key_words:
    def __init__(self):

        # self.file_path_list = ['../rawdata/中图学报2013-2019.txt', '../rawdata/中图学报2008-2012.txt',
        #                        '../rawdata/中图学报2005-2008.txt', '../rawdata/中图学报2002-2004.txt',
        #                        '../rawdata/中图学报2000-2001.txt']

        # self.file_path_list = ['../rawdata/情报学报1.txt', '../rawdata/情报学报2.txt', '../rawdata/情报学报3.txt',
        #                        '../rawdata/图书与情报.txt']

        self.file_path_list = ['../rawdata/图书馆论坛1.txt', '../rawdata/图书馆论坛2.txt', '../rawdata/图书馆论坛3.txt',
                               '../rawdata/图书馆论坛4.txt', '../rawdata/图书馆论坛5.txt', '../rawdata/图书馆论坛6.txt',
                               '../rawdata/图书馆论坛7.txt', '../rawdata/图书馆论坛8.txt', '../rawdata/图书馆论坛9.txt',
                               '../rawdata/图书馆论坛10.txt', '../rawdata/图书馆论坛11.txt', '../rawdata/图书馆论坛12.txt']
        self.kw_data = set()  # 不重复集合
        self.db_operate = kw_to_mongo()

    def get_file_kw_endnote(self, file_name):
        text_file = open(file_name, 'r', encoding='utf-8')
        while True:
            line = text_file.readline()
            pattern = r'%K\s+([^;]*;;)*([^;]*$)'
            result = re.search(pattern, line)
            if result:
                kw_patterm = r'([^\s;]+)'
                line = line.replace('%K', '')
                kw = re.findall(kw_patterm, line)
                for k in kw:
                    self.kw_data.add(k)
            if not line:
                break

    def get_file_kw_eln(self):
        text_file = open('../rawdata/中图学报2000-2001.eln', 'r', encoding='utf-8')
        text = text_file.read()
        # while True:
        #     line = text_file.readline()
        #     print(line)
        #     if not line:
        #         break
        parser = xml.sax.make_parser()

    def get_all_kw_to_db(self):
        for path in self.file_path_list:
            self.get_file_kw_endnote(path)
        # print(len(self.kw_data))
        # print(self.kw_data)
        for k in self.kw_data:
            self.db_operate.save_kw(k)

    def get_all_from_db(self):
        self.db_operate.get_all()


if __name__ == '__main__':
    kw_op = key_words()
    # kw_op.get_all_kw_to_db()
    kw_op.get_all_from_db()
    # kw_op.get_file_kw_eln()
