import time
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

driver = webdriver.Chrome()
driver.maximize_window()
driver.implicitly_wait(6)
print(By)
driver.get("http://www.baidu.com/")
driver.find_element_by_css_selector('#kw').send_keys('图书')
driver.find_element_by_css_selector('#su').click()
try:
    element = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.ID, "1"))
    )
    print(element)
except Exception as e:
    print(e)

# result

# time.sleep(1)
# js = 'window.open("http://172.16.0.1");'
# driver.execute_script(js)
# time.sleep(2)
