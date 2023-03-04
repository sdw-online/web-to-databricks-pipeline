import requests
from dotenv import load_dotenv
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from time import sleep
import pandas as pd
import os


prem_league_table_url = 'https://www.twtd.co.uk/league-tables/competition:premier-league/daterange/fromdate:2022-Jul-01/todate:2023-Jan-01/type:home-and-away/'

target_path    =   os.path.abspath('scraper/temp_storage')


match_weeks = [week_no for week_no in range(1, 39)]
match_week_date = []

options = webdriver.ChromeOptions()
options.add_argument("--start-maximized")
chrome_driver = webdriver.Chrome(ChromeDriverManager().install(), options=options)


try:
    chrome_driver.get(prem_league_table_url)
    wait = WebDriverWait(chrome_driver, 5)
    close_cookie_box = wait.until(EC.presence_of_element_located((By.XPATH, '/html/body/div[8]/div[2]/div[1]/div[1]/button/i')))
    close_cookie_box.click()



    table = chrome_driver.find_element(By.CLASS_NAME, 'leaguetable')
    table_rows = table.find_elements(By.XPATH, './/tr')
    scraped_content = []



    for table_row in table_rows:
        cells = table_row.find_elements(By.TAG_NAME, 'td')
        row_data = []
        for cell in cells:
            row_data.append(cell.text)
        scraped_content.append(row_data)

    df = pd.DataFrame(data=scraped_content[1:], columns=[scraped_content[0]])

    print(df)
    df.to_csv(f'{target_path}/prem_league_table.csv', index=False)


    sleep(600)

except Exception as e:
    print(e)