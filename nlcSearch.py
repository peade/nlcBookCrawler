from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.select import Select
import time
from pyquery import PyQuery as pq
from mongoOperate import dataToMongo

dbop = dataToMongo()

driver = webdriver.Chrome()

def page_load():
    try:
        # WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "div")))
        driver.implicitly_wait(10)
        driver.get('http://ucs.nlc.cn')
        more = driver.find_element_by_link_text('更多选项')
        more.click()
        # 字段选择
        cat = driver.find_element_by_id("find_code")
        Select(cat).select_by_index(9)
        # 搜索框
        input = driver.find_element_by_id("reqterm")
        input.send_keys('G25*')
        # 文献
        driver.find_element_by_id("local_base").click()
        # 语言
        lan = driver.find_element_by_name('filter_request_1')
        Select(lan).select_by_index(1)
        # 资料
        material = driver.find_element_by_name('filter_request_4')
        Select(material).select_by_index(1)
        # 开始时间
        startTime = driver.find_element_by_name('filter_request_2')
        startTime.send_keys('2000')
        # 结束时间
        endTime = driver.find_element_by_name('filter_request_3')
        endTime.send_keys('2000')
        # 发起搜索
        driver.find_element_by_css_selector('input[type="submit"] ').click()
        driver.implicitly_wait(10)

        # 页面里多个数据
        page()
        getInfo()
        getNextPage()
        time.sleep(60 * 5)
        driver.close()
    except Exception as err:
        print(err)
    finally:
        print('finally')
        # driver.quit()


# 获取下一页按钮
def getNextPage():
    while True:
        try:
            active = driver.find_element_by_css_selector('.curpage').text
            print('已完成页面' + active)
            next = driver.find_element_by_css_selector('#nav a:last-of-type')
            next.click()
            page()
            getInfo()
        except Exception:
            break


# 搜索结果页面 图书链接
def page():
    driver.implicitly_wait(10)
    # driver.find_element_by_tag_name('body').send_keys(Keys.CONTROL + 't')
    alist = driver.find_elements_by_css_selector('.itemtitle a')
    # for i in range(len(alist)):
    # driver.find_element_by_tag_name('body').send_keys(Keys.CONTROL + 't')
    for a in alist:
        link = a.get_attribute("href")
        print(link)
        getItemBook(link)
    # getItemBook(alist[0].get_attribute("href"))


def getInfo():
    driver.find_element_by_tag_name('html').send_keys(Keys.CONTROL + 't')
    print('info data')


def getItemBook(link):
    js = 'window.open("' + link + '");'
    driver.execute_script(js)
    time.sleep(0.2)
    driver.switch_to.window(driver.window_handles[1])
    cur = driver.find_element_by_id('fmtname')
    webdriver.ActionChains(driver).move_to_element(cur).perform()
    marc = driver.find_element_by_id('fmt_marc')
    marc.click()
    # html = driver.page_source
    # doc = pq(html)
    time.sleep(0.1)
    trs = driver.find_elements_by_css_selector('#details2 tbody tr')

    # field001 = ''
    # field010 = ''
    # field200 = ''
    # field205 = ''
    # field210 = ''
    # field215 = ''
    # field330 = ''
    # field6 = ''
    # field7 = ''
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
    dbop.saveData(dict)
    print(dict)

    driver.close()
    driver.switch_to.window(driver.window_handles[0])


if __name__ == "__main__":
    page_load()
