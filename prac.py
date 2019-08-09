import time
from selenium import webdriver
from selenium.webdriver.common.keys import Keys

driver = webdriver.Chrome()
driver.maximize_window()
driver.implicitly_wait(6)

driver.get("http://www.baidu.com/")
time.sleep(1)
js = 'window.open("http://172.16.0.1");'
driver.execute_script(js)
time.sleep(2)
