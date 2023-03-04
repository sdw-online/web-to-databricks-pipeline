import requests
from dotenv import load_dotenv
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait


url = 'https://www.twtd.co.uk/league-tables/competition:premier-league/daterange/fromdate:2022-Jul-01/todate:2023-Jan-01/type:home-and-away/'

options = webdriver.ChromeOptions()
options.add_argument("--start-maximized")
chrome_driver = webdriver.Chrome(ChromeDriverManager().install(), options=options)

# chrome_driver = webdriver.Chrome(options=options)

try:
    chrome_driver.get(url)
    chrome_driver.find_element(By.XPATH, '/html/body/div[8]/div[2]/div[1]/div[1]/button/i')
    chrome_driver.click()
except Exception as e:
    print(e)





