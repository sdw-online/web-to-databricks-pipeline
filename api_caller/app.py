import io
import os
import json
import boto3
import requests
import pandas as pd
from time import sleep
from pathlib import Path
import logging, coloredlogs
from dotenv import load_dotenv


# ================================================ LOGGER ================================================


# Set up root root_logger 
root_logger     =   logging.getLogger(__name__)
root_logger.setLevel(logging.DEBUG)


# Set up formatter for logs 
file_handler_log_formatter      =   logging.Formatter('%(asctime)s  |  %(levelname)s  |  %(message)s  ')
console_handler_log_formatter   =   coloredlogs.ColoredFormatter(fmt    =   '%(message)s', level_styles=dict(
                                                                                                debug           =   dict    (color  =   'white'),
                                                                                                info            =   dict    (color  =   'green'),
                                                                                                warning         =   dict    (color  =   'cyan'),
                                                                                                error           =   dict    (color  =   'red',      bold    =   True,   bright      =   True),
                                                                                                critical        =   dict    (color  =   'black',    bold    =   True,   background  =   'red')
                                                                                            ),

                                                                                    field_styles=dict(
                                                                                        messages            =   dict    (color  =   'white')
                                                                                    )
                                                                                    )


# Set up file handler object for logging events to file
current_filepath    =   Path(__file__).stem
file_handler        =   logging.FileHandler('logs/api_caller/' + current_filepath + '.log', mode='w')
file_handler.setFormatter(file_handler_log_formatter)


# Set up console handler object for writing event logs to console in real time (i.e. streams events to stderr)
console_handler     =   logging.StreamHandler()
console_handler.setFormatter(console_handler_log_formatter)


# Add the file handler 
root_logger.addHandler(file_handler)


# Only add the console handler if the script is running directly from this location 
if __name__=="__main__":
    root_logger.addHandler(console_handler)






# ================================================ CONFIG ================================================

# Load environment variables to session
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





# Set up other constants

API_KEY             =       os.getenv("API_KEY")
API_HOST            =       os.getenv("API_HOST")
league_id           =       os.getenv("LEAGUE_ID")
season              =       os.getenv("SEASON")
team_id             =       os.getenv("TEAM_ID")
match_date          =       '2022-10-10'

teams_url           =       f"https://api-football-v1.p.rapidapi.com/v3/teams/statistics?league={league_id}&team={team_id}&season={season}&date={match_date}"
headers             =       {"X-RapidAPI-Key": API_KEY, "X-RapidAPI-Host": API_HOST}
query_string        =       {'league': league_id, 'season': season, 'team': team_id}




# Send HTTP request for football data to Rapid-API endpoint
try:
    response        = requests.request("GET", teams_url, headers=headers, params=query_string)

    # Display the response in a readable JSON format
    response_json = json.dumps(response.json(), indent=4)
    # root_logger.debug(response_json)


    # Read JSON payload into data frame 
    fixtures = response.json()['response']['fixtures']
    # print(fixtures)

    df = pd.json_normalize(fixtures)

    print(df)

    # df = pd.concat([df.drop(['played'], axis=1 ), pd.json_normalize(played)], axis=1)
    
    # wins = fixtures['wins']
    # df = pd.concat([df.drop(['wins'], axis=1 ), pd.json_normalize(wins)], axis=1)

    # draws = fixtures['draws']
    # df = pd.concat([df.drop(['draws'], axis=1 ), pd.json_normalize(draws)], axis=1)
    
    # loses = fixtures['loses']
    # df = pd.concat([df.drop(['loses'], axis=1 ), pd.json_normalize(loses)], axis=1)


    local_target_path               =   os.path.abspath('api_caller/temp_storage')
    team_file                       =   f'team_{match_date}.csv'
    print(df)
    df.to_csv(f'{local_target_path}/{team_file}' , index=False)

except Exception as e:
    root_logger.error(e)




