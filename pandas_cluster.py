import pandas as pd
from sklearn.cluster import KMeans

data = pd.read_csv('./rawdata/test.csv', sep=',', index_col=0, encoding='gbk')
matrix = data[
    ['研究', '中国', '信息检索', '文集', '教材', '图书馆工作', '高等学校', '院校图书馆', '图书馆学', '古籍', '读书活动', '互联网络', '图书馆管理', '古文献学', '图书馆服务',
     '数字图书馆', '公共图书馆', '图书史', '图书馆事业', '高等教育', '科技情报', '医学', '民国', '文献学', '应用', '汇编', '图书馆', '介绍', '基本知识', '计算机网络',
     '概况', '世界', '期刊', '情报学', '情报服务', '图书情报工作', '善本', '版本', '史料', '丛书', '信息资源', '藏书楼', '古代', '丛刊', '社会科学', '藏书', '信息工作',
     '现代', '古籍整理', '古籍研究', '文献', '私人藏书', '手册', '图书馆事业史', '概论', '清代', '信息管理', '医学院校', '工具书', '网络检索', '目录学', '化学',
     '计算机应用', '图书馆史', '高等职业教育', '研究报告', '写作', '少数民族', '论文', '化学工业', '版本学', '书影', '文献计量学', '市场竞争', '规范', '资源建设', '市级图书馆',
     '企业管理', '专利', '清后期', '农村图书馆', '图录', '数字技术', '学校图书馆', '省级图书馆', '读者工作', '文化史', '机器可读目录', '阅读辅导', '编目规则', '文献资源建设',
     '中小学', '索引', '儿童图书馆', '图书', '云南', '美国', '题跋', '北京', '校勘学']].as_matrix()
kms = KMeans(n_clusters=5)
y = kms.fit_predict(matrix)
print(y)
