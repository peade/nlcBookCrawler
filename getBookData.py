import traceback
from selenium import webdriver
from selenium.common import exceptions
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.select import Select
from selenium.webdriver.chrome.options import Options
import time
from pyquery import PyQuery as pq
from mongoOperate import dataToMongo

chrome_options = Options()
chrome_options.add_argument('blink-settings=imagesEnabled=false')
chrome_options.add_argument('--headless')


class nlcBookData:
    dbop = dataToMongo()
    driver = webdriver.Chrome(chrome_options=chrome_options)

    # driver = webdriver.Chrome()

    def __init__(self):
        self.isFinish = False
        self.liskLink = ''
        self.year = 2000
        self.keyword = ''
        self.index = 0

    def setKeyword(self, keyword):
        self.keyword = keyword

    def loadPage(self, startIdx, year):
        self.isFinish = False
        self.year = year
        print(self.keyword, self.year)
        link = ''
        try:
            self.driver.get('http://ucs.nlc.cn')
            more = self.driver.find_element_by_link_text('更多选项')
            more.click()
            # 开始搜索
            self.startSearch(self.year)
            link = self.driver.find_element_by_css_selector('#nav a').get_attribute('href')
        except exceptions.NoSuchElementException as e:
            print(e)
        except exceptions.TimeoutException as e:
            print(e)
        try:
            if link:
                self.liskLink = link.replace('jump=11', 'jump=')
                for i in range(startIdx, 101):
                    self.index = i
                    print('__________' + str(i) + '_________')
                    if self.isFinish:
                        self.index = 100
                        self.driver.quit()
                        # self.driver = webdriver.Chrome()
                        self.driver = webdriver.Chrome(chrome_options=chrome_options)
                        break
                    self.goToPage(i)
            else:
                self.getBookLink()
                # self.isFinish = True
                self.index = 100
                self.driver.quit()
                # self.driver = webdriver.Chrome()
                self.driver = webdriver.Chrome(chrome_options=chrome_options)

        except Exception:
            # self.driver.close()
            self.driver.quit()
            # self.driver = webdriver.Chrome()
            self.driver = webdriver.Chrome(chrome_options=chrome_options)
            print(Exception)
            traceback.print_exc()

        # try:
        #     link = self.driver.find_element_by_css_selector('#nav a').get_attribute('href')
        #     self.liskLink = link.replace('jump=11', 'jump=')
        #     for i in range(self.startIdx, 100):
        #         print('__________' + str(i) + '_________')
        #         if self.isFinish:
        #             self.driver.close()
        #             break
        #         self.goToPage(i)
        # except:
        #     self.getBookLink()

    def startSearch(self, year):

        # 字段选择
        cat = self.driver.find_element_by_id("find_code")
        Select(cat).select_by_index(9)
        # 搜索框
        input = self.driver.find_element_by_id("reqterm")
        input.send_keys(self.keyword)
        # 文献
        self.driver.find_element_by_id("local_base").click()
        # 语言
        lan = self.driver.find_element_by_name('filter_request_1')
        Select(lan).select_by_index(1)
        # 资料
        material = self.driver.find_element_by_name('filter_request_4')
        Select(material).select_by_index(1)
        # 开始时间
        startTime = self.driver.find_element_by_name('filter_request_2')
        startTime.send_keys(year)
        # 结束时间
        endTime = self.driver.find_element_by_name('filter_request_3')
        endTime.send_keys(year)
        # 发起搜索
        self.driver.find_element_by_css_selector('input[type="submit"] ').click()

    def goToPage(self, num):
        s = str(num * 10 + 1)
        js = 'window.open("' + self.liskLink + s + '", "_self");'
        self.driver.execute_script(js)
        self.getBookLink()

    def getBookLink(self):
        alist = self.driver.find_elements_by_css_selector('.itemtitle a')
        print('页面图书数量：', len(alist))
        if len(alist) < 10:
            self.isFinish = True
        for a in alist:
            link = a.get_attribute("href")
            # print(link)
            self.getBookData(link)

    def getBookData(self, link):
        js = 'window.open("' + link + '");'
        self.driver.execute_script(js)
        self.driver.switch_to.window(self.driver.window_handles[1])
        time.sleep(0.2)
        WebDriverWait(self.driver, 20).until(
            EC.presence_of_element_located((By.ID, "fmtname"))
        )
        cur = self.driver.find_element_by_id('fmtname')
        webdriver.ActionChains(self.driver).move_to_element(cur).perform()
        time.sleep(0.2)
        marc = self.driver.find_element_by_id('fmt_marc')
        marc.click()

        time.sleep(1)

        trs = self.driver.find_elements_by_css_selector('#details2 tbody tr')
        # WebDriverWait(self.driver, 10).until(
        #     EC.presence_of_element_located((By.CSS_SELECTOR, "#details2 tbody tr td"))
        # )
        # trtd = lambda x: x.find_elements_by_css_selector("td")
        # WebDriverWait(self.driver, 10).until(trtd(trs))
        dict = {
            '_id': '',  # 001
            'isbn': '',  # 010
            'name': '',  # 2001
            'version': '',  # 205
            'publisher': '',  # 210
            'pages': '',  # 215
            'series': '',  # 300 丛书项
            'abstract': '',  # 330 摘要
            'subject_terms': '',  # 6 主题词
            'author': ''  # 7 作者
        }
        for tr in trs:
            tds = tr.find_elements_by_css_selector('td')
            code = tds[0].text
            val = tds[1].text
            if code.startswith('001'):
                dict['_id'] = val
            if code.startswith('010'):
                dict['isbn'] = val
            if code.startswith('200'):
                dict['name'] = val
            if code.startswith('205'):
                dict['version'] = val
            if code.startswith('210'):
                dict['publisher'] = val
            if code.startswith('215'):
                dict['pages'] = val
            if code.startswith('300'):
                dict['series'] = val
            if code.startswith('330'):
                dict['abstract'] = val
            if code.startswith('6'):
                dict['subject_terms'] += '$$' + val
            if code.startswith('7'):
                dict['author'] += '$$' + val
        if '全国图书馆文献缩微中心' not in dict['publisher']:
            self.dbop.saveData(dict)
            print(dict)

        self.driver.close()
        self.driver.switch_to.window(self.driver.window_handles[0])


if __name__ == "__main__":
    action = nlcBookData()
    action.index = 0
    # '0?', '1?', '2?', '3?', '4?', '5?', '6?', '7?', '8?', '9?', '-?', '?',
    keyword = ['0*', '1*', '2*', '3*', '4*', '5*', '6*', '7*', '8*', '9*', '-*', '']
    # keyword = ['*']
    kwIdx = 0
    year = 2015
    while True:
        if action.index == 100:
            print('finish', action.index)
            action.index = 0
            year += 1
            # kwIdx += 1
            # if kwIdx >= len(keyword):
            #     kwIdx = 0
            #     year += 1
        if year > 2018:
            break
        # print(action.index, year, kwIdx, 'G25' + keyword[kwIdx])
        # kw = 'G35' + keyword[kwIdx]
        kw = 'G25*'
        action.setKeyword(kw)
        action.loadPage(action.index, year)

        # try:
        #     action.loadPage(action.index, year)
        # except Exception as e:
        #     if e.message:
        #         print(e.message)
        #     else:
        #         print(e)
