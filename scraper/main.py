import requests
from dotenv import load_dotenv
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from time import sleep



url = 'https://www.twtd.co.uk/league-tables/competition:premier-league/daterange/fromdate:2022-Jul-01/todate:2023-Jan-01/type:home-and-away/'

options = webdriver.ChromeOptions()
options.add_argument("--start-maximized")
chrome_driver = webdriver.Chrome(ChromeDriverManager().install(), options=options)

try:
    chrome_driver.get(url)
    wait = WebDriverWait(chrome_driver, 5)
    close_cookie_box = wait.until(EC.element_to_be_clickable((By.XPATH, '/html/body/div[8]/div[2]/div[1]/div[1]/button/i')))
    close_cookie_box.click()

    table = chrome_driver.find_element(By.CLASS_NAME, 'leaguetable')
    rows = table.find_elements(By.XPATH, './/tr')

    for row in rows:
        cells = row.find_elements(By.XPATH, './/td')
        for cell in cells:
            print(cell.text)

    sleep(600)

except Exception as e:
    print(e)