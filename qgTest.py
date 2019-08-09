from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

driver = webdriver.Chrome()


def page_load():
    try:
        # WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "div")))
        driver.implicitly_wait(10)
        driver.get('https://www.xuexi.cn/')
        loginBtn = driver.find_element_by_css_selector('.login-icon')
        # loginBtn.click()
        # WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "div")))
        # print('find div')
    except Exception as err:
        print(err)
    finally:
        print('finally')
        # driver.quit()


if __name__ == "__main__":
    page_load()
