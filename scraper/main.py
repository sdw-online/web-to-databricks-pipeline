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


target_path             =   os.path.abspath('scraper/temp_storage')
match_dates             =   ['2022-Sep-01', '2022-Oct-01', '2022-Nov-01', '2022-Dec-01', '2023-Jan-01', '2023-Feb-01', '2023-Mar-01' ]


options = webdriver.ChromeOptions()
options.add_argument("--start-maximized")
chrome_driver = webdriver.Chrome(ChromeDriverManager().install(), options=options)

for match_date in match_dates:
    try:
        prem_league_table_url   = f'https://www.twtd.co.uk/league-tables/competition:premier-league/daterange/fromdate:2022-Jul-01/todate:{match_date}/type:home-and-away/'

        chrome_driver.get(prem_league_table_url)
        try:
            wait = WebDriverWait(chrome_driver, 5)
            close_cookie_box = wait.until(EC.presence_of_element_located((By.XPATH, '/html/body/div[8]/div[2]/div[1]/div[1]/button/i')))
            close_cookie_box.click()
        except Exception as e:
            continue

        table = chrome_driver.find_element(By.CLASS_NAME, 'leaguetable')
        table_rows = table.find_elements(By.XPATH, './/tr')
        scraped_content = []

        for table_row in table_rows:
            cells = table_row.find_elements(By.TAG_NAME, 'td')
            row_data = []
            for cell in cells:
                row_data.append(cell.text)
            scraped_content.append(row_data)

        prem_league_table_df = pd.DataFrame(data=scraped_content[1:], columns=[scraped_content[0]])

        print(prem_league_table_df)
        prem_league_table_df.to_csv(f'{target_path}/prem_league_table_{match_date}.csv', index=False)

        # sleep(600)

    except Exception as e:
        print(e)