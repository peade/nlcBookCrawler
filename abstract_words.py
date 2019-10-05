# 处理标题
import pymongo
import re
import json
import csv
import jieba
import jieba.analyse


class abstract_words:
    dbClient = pymongo.MongoClient('mongodb://localhost:27017/')
    db = dbClient['bookdb']
    book_col = db['book']
    word_col = db['key_words']
    title_col = db['title']
    year_array = ['2000', '2001', '2002', '2003', '2004', '2005', '2006', '2007', '2008', '2009', '2010', '2011',
                  '2012', '2013', '2014', '2015', '2016', '2017', '2018']
    gap_array = ['2000-2004', '2005-2009', '2010-2014', '2015-2018']

    # 列出图书信息
    def list_book_info(self):
        all = self.book_col.find()
        for book in all:
            print(book['title_words'])

    # 列出所有的表 （数据集合）
    def list_col(self):
        all_col = self.db.list_collection_names()
        for name in all_col:
            print(name)

    # 列出自定义关键词
    def list_cust_key_word(self):
        all = self.word_col.find()
        for word in all:
            print(word['word'])

    # 删除错误主题词
    def cust_key_clear(self):
        clean_arr = [2, 4, 21, 27000, '《汉语主题词表》(工程技术版)', '《中国图书馆分类法(儿童版)》', '《中国图书馆分类法》(第4版)',
                     '《中国图书馆分类法》(第5版)', '《中国图书馆分类法》(第五版)', '《中国图书馆学报》50周年', '《中图法》(第五版)', '《中图法》(四版)', '《中图法》Web版',
                     '《中图法》编委会', '《中图法》第4版', '《中图法》第5版', '《中图法》第四版', '《中图法》第五版', '《中图法》电子版', '《中图法》四版', '《中图法》体系和方法',
                     '《中图法》修订', '《中图法》循证医学', '《庄子》的传播与接受', '版权贸易体制改革', '编目思想,资源共享', '参考咨询与信息服务', '当代社会科学理论',
                     '的146种,4个主题词的47种,5个主题词的6种', '地方志索引编制规则', '第四种情报检索语言系统', '电子图书馆藏发展', '电子图书与图书馆', '电子文件管理元数据',
                     '电子信息资源数据库', '电子政务门户', '电子政务信息资源', '读者服务升级计划', '读者关系管理(RRM)', '读者驱动采购', '多媒体大学图书馆', '法定许可使用权主体',
                     '法律预测研究', '法人治理结构', '反竞争情报体系模型', '泛在图书馆环境', '非物质文化遗产数据库', '非相关文献知识发现', '非正式信息交流行为', '服务绩效评估指标体系',
                     '覆盖最多的为6个主题词,但', '高等教育信息素养框架', '高等学校图书馆', '高等学校资料室', '公共图书馆服务体系', '公共图书馆评估定级', '公共图书馆制度的价值',
                     '公益性开发和服务', '管理现状分析', '广佛同城公共图书馆服务体系', '国际标准书目著录(电子资源)', '国家图书馆主干网', '合作数字参考咨询服务', '基本公共文化服务标准',
                     '基于作者关键词耦合分析的研究专业识别', '李燕亭图书馆学著译整理与研究', '理论与实践', '理论与实践的关系', '理论与实践结合', '历史作用与局限', '联合参考咨询服务',
                     '美国公共图书馆', '民族地区高校图书馆', '民族地区图书馆', '目', '欧美中公共信息管理', '评价指标体系', '裘开明图书馆学论文选集', '人', '人事管理艺术',
                     '人文社会科学研究', '入职门槛职称评定', '弱势群体公共图书馆服务', '社区图书馆服务对象', '社区图书馆功能定位', '社区图书馆建设', '社区图书馆建设模式',
                     '师生关系步长名师效应', '数据库建设与转型', '数据驱动的科学', '数据质量影响因素模型', '数字化、虚拟化馆藏', '数字时代的图书馆信息服务', '数字图书馆服务登记系统',
                     '数字信息资源的检索与利用', '数字资源长期保存', '特定专业领域杰出人才', '特色优势学科图书馆', '图书馆:图书馆规程', '图书馆服务基本原则', '图书馆工作实践',
                     '图书馆宏观管理', '图书馆情报学教育', '图书馆情报学学科', '图书馆时尚阅读推广', '图书馆网络信息资源', '图书馆危机管理', '图书馆信息资源建设', '图书馆学档案学',
                     '图书馆学发展', '图书馆学基础理论', '图书馆学教育改革', '图书馆学理论基础', '图书馆学情报学', '图书馆学情报学核心期刊', '图书馆学情报学教育', '图书馆学研究对象',
                     '图书馆学专业核心课程', '图书馆与先进文化', '图书馆员职业价值', '图书馆专业人才培养', '图书情报学虚拟图书馆', '先进文化前进方向:社会主义精神文明', '现代信息通讯技术',
                     '辛亥以来藏书纪事诗', '信息传播方式', '信息服务:网络化', '学科信息资源优化', '研究服务/科研服务', '用户体验优化', '与贸易有关的知识产权协议', '阅读图书馆阅读推广',
                     '中国古代私家藏书', '中国图书馆分类法(第五版)', '重点学科信息导航',
                     ]
        for word in clean_arr:
            self.word_col.delete_one({'word': word})
        print('finish clear key words')

    # 分割标题
    def cut_title(self):
        books = self.book_col.find()
        all_word = self.word_col.find()
        # jieba分词，添加自定义词
        for word in all_word:
            jieba.add_word(word['word'])
        # add_words = ['举要']
        del_words = ['的', '与', '《', '》', '及', '·', '(', ')', '叫', '“', '”', '是', '从', '、']
        for word in del_words:
            jieba.del_word(word)
        for book in books:
            res = jieba.lcut(book['bookname'], cut_all=False)
            title_words = '|'.join(res)
            self.book_col.update_one({'_id': book['_id']}, {"$set": {'title_words': title_words}})

    # 标题分词到数据表
    def fill_title(self):
        all = self.book_col.find()
        del_words = ['的', '与', '《', '》', '及', '·', '(', ')', '叫', '“', '”', '是', '从', '、']
        # 主要款目集合
        word_set = set()
        # 图书数据数组 从数据库里查出来的值，只能进行一次遍历
        book_list = []
        for book in all:
            if 'subject_terms' in book:
                title_text = book['title_words']
                result = set(title_text.split('|'))
                for dw in del_words:
                    if dw in result:
                        result.remove(dw)
                book['title_word_list'] = result
                book_list.append(book)
                temp_set = set(result)
                word_set = word_set.union(temp_set)
        # print(word_set)
        # 构建主题词键值对，每一个主题词下面，有各个年份的初始数量为0
        title_dict = dict()
        for sub in word_set:
            temp_dict = {'word': sub, 'total': 0}
            for year in self.year_array:
                temp_dict[year] = 0
            title_dict[sub] = temp_dict
        for book in book_list:
            for word in book['title_word_list']:
                title_dict[word][book['pub_year']] += 1
                title_dict[word]['total'] += 1
        # 将数据批量存进数据库
        data_list = []
        for title_item in title_dict:
            title_dict[title_item]['gap1'] = title_dict[title_item]['2000'] + title_dict[title_item]['2001'] + \
                                             title_dict[title_item]['2002'] + title_dict[title_item]['2003'] + \
                                             title_dict[title_item]['2004']
            title_dict[title_item]['gap2'] = title_dict[title_item]['2005'] + title_dict[title_item]['2006'] + \
                                             title_dict[title_item]['2007'] + title_dict[title_item]['2008'] + \
                                             title_dict[title_item]['2009']
            title_dict[title_item]['gap3'] = title_dict[title_item]['2010'] + title_dict[title_item]['2011'] + \
                                             title_dict[title_item]['2012'] + title_dict[title_item]['2013'] + \
                                             title_dict[title_item]['2014']
            title_dict[title_item]['gap4'] = title_dict[title_item]['2015'] + title_dict[title_item]['2016'] + \
                                             title_dict[title_item]['2017'] + title_dict[title_item]['2018']

            data_list.append(title_dict[title_item])
        # for da in data_list:
        #     print(da)
        # print(title_dict)
        # print(data_list)

        # 保持数据到数据库
        self.title_col.insert_many(data_list)

    # 高频标题词
    def title_top(self):
        all = self.title_col.find().limit(100).sort('total', pymongo.DESCENDING)
        print(all.count())
        for word in all:
            print(word['word'], word['total'])

    # 清空标题词表
    def clean_title(self):
        self.title_col.remove({})

    # 分割摘要
    def cut_abstract(self):
        books = self.book_col.find().limit(100)
        all_word = self.word_col.find()
        # jieba分词，添加自定义词
        for word in all_word:
            jieba.add_word(word['word'])
        for book in books:
            # res = jieba.lcut(book['bookname'], cut_all=False)
            # print(book['bookname'], res)
            abs_tag = jieba.analyse.extract_tags(book['abstract'], topK=30, withWeight=False)
            abs_cut = jieba.lcut(book['abstract'], cut_all=False)
            abs_rank_tag = jieba.analyse.textrank(book['abstract'], topK=20, withWeight=False,
                                                  allowPOS=('ns', 'n', 'vn', 'v'))
            print(abs_tag)
            # print(abs_cut)
            print('rank', abs_rank_tag)


if __name__ == "__main__":
    title = title_process()
    title.title_top()
    # title.cut_title()
