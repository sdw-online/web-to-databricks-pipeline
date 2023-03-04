
import os
import boto3
import pandas as pd
from time import sleep
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support import expected_conditions as EC

# Load environment variables into session 
load_dotenv()


# Set up environment variables

## AWS 

ACCESS_KEY                          =   os.getenv("ACCESS_KEY")
SECRET_ACCESS_KEY                   =   os.getenv("SECRET_ACCESS_KEY")
REGION_NAME                         =   os.getenv("REGION_NAME")
S3_BUCKET                           =   os.getenv("S3_BUCKET")
S3_FOLDER                           =   os.getenv("S3_FOLDER")


# Set up constants for S3 file to be imported
s3_client                                           =       boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_ACCESS_KEY, region_name=REGION_NAME)


# Create S3 bucket if it doesn't exist
s3_response = s3_client.list_buckets()
print(s3_response['Buckets'])


try:
    s3_client.create_bucket(Bucket=S3_BUCKET)
    print(f'Successfully created "{S3_BUCKET}" bucket in AWS S3.')
except Exception as e:
    print(e)
    # print(f'The "{S3_BUCKET}" bucket already exists ...  ')


# Specify the constants for the scraper 
local_target_path               =   os.path.abspath('scraper/temp_storage')
match_dates                     =   ['2022-Sep-01', '2022-Oct-01', '2022-Nov-01', '2022-Dec-01', '2023-Jan-01', '2023-Feb-01', '2023-Mar-01' ]



# # Set up the Selenium Chrome driver 
# options = webdriver.ChromeOptions()
# options.add_argument("--start-maximized")
# chrome_driver = webdriver.Chrome(ChromeDriverManager().install(), options=options)








# # Begin scraping 
# for match_date in match_dates:
#     try:
#         prem_league_table_url   =   f'https://www.twtd.co.uk/league-tables/competition:premier-league/daterange/fromdate:2022-Jul-01/todate:{match_date}/type:home-and-away/'
#         chrome_driver.get(prem_league_table_url)
#         print(f'>>>> Running Prem League link for {match_date}...')
#         print(f'>>>> ')
#         try:
#             wait = WebDriverWait(chrome_driver, 5)
#             close_cookie_box    =   wait.until(EC.presence_of_element_located((By.XPATH, '/html/body/div[8]/div[2]/div[1]/div[1]/button/i')))
#             close_cookie_box.click()
#             print(f'>>>> Closing cookie pop-up window ...')
#             print(f'>>>> ')
#         except Exception as e:
#             print(f'No cookie pop-up window to close...let\'s begin scraping for "{match_date}" match week !!')

#         table               =   chrome_driver.find_element(By.CLASS_NAME, 'leaguetable')
#         table_rows          =   table.find_elements(By.XPATH, './/tr')
#         scraped_content     =   []
#         print(f'>>>> Extracting content from HTML elements... {match_date}...')
#         print(f'>>>> ')

#         for table_row in table_rows:
#             cells           =   table_row.find_elements(By.TAG_NAME, 'td')
#             row_data        =   []
#             cell_counter    =   0
#             for cell in cells:
#                 cell_counter += 1
#                 row_data.append(cell.text)
#                 print(f'>>>> Cell no {cell_counter} appended ...')
#                 print(f'>>>> ')
#             scraped_content.append(row_data)


#         # Use HTML content to create data frame for the Premier League table standings 
#         prem_league_table_df    =   pd.DataFrame(data=scraped_content[1:], columns=[scraped_content[0]])

#         print(prem_league_table_df)

#         # Write data frame to CSV file
#         prem_league_table_df.to_csv(f'{local_target_path}/prem_league_table_{match_date}.csv', index=False)
#         print(f'>>>> Successfully written "prem_league_table_{match_date}.csv" to target location... {match_date}...')
#         print(f'>>>> ')


#         # Add delays to avoid overloading the website's servers 
#         sleep(3)

#     except Exception as e:
#         print(e)