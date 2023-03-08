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



# Add a flag for saving CSV files to the cloud 
WRITE_TO_CLOUD = False


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

API_KEY                         =       os.getenv("API_KEY")
API_HOST                        =       os.getenv("API_HOST")
league_id                       =       os.getenv("LEAGUE_ID")
season                          =       os.getenv("SEASON")
team_id                         =       os.getenv("TEAM_ID")
local_target_path               =       os.path.abspath('api_caller/temp_storage/teams/dirty_data')
match_dates                     =       ['2022-09-01', '2022-10-01', '2022-11-01', '2022-12-01', '2023-01-01', '2023-02-01', '2023-03-01', '2023-03-07']
headers                         =       {"X-RapidAPI-Key": API_KEY, "X-RapidAPI-Host": API_HOST}
query_string                    =       {'league': league_id, 'season': season, 'team': team_id}



for match_date in match_dates:
    
    try:

        # Send HTTP request for football data to Rapid-API endpoint
        teams_url                                       =       f"https://api-football-v1.p.rapidapi.com/v3/teams/statistics?league={league_id}&team={team_id}&season={season}&date={match_date}"
        team_file                                       =       f'team_{match_date}.csv'
        S3_KEY                                          =       S3_FOLDER + team_file
        CSV_BUFFER                                      =       io.StringIO()

        
        root_logger.info(f'>>>>   Sending HTTP GET requests to API endpoint for team profile as of {match_date} ...')
        root_logger.debug(f'>>>>   ')
        response                        =       requests.request("GET", teams_url, headers=headers, params=query_string)

        # Display the response in a readable JSON format
        root_logger.info(f'>>>>   Requests completed, now processing JSON payload ...')
        root_logger.debug(f'>>>>   ')
        response_json = json.dumps(response.json(), indent=4)


        # Read JSON payload into data frame 
        root_logger.info(f'>>>>   Reading JSON payload into data frame ...')
        root_logger.debug(f'>>>>   ')
        fixtures = response.json()['response']['fixtures']
        df = pd.json_normalize(fixtures)
        df['match_date'] = match_date
        print(df)


        # Write data frame to CSV file
        root_logger.info(f'>>>>   Writing data frame to CSV file ...')
        root_logger.debug(f'>>>>   ')




        if WRITE_TO_CLOUD:
            df.to_csv(CSV_BUFFER , index=False)
            RAW_TABLE_ROWS_AS_STRING_VALUES              =       CSV_BUFFER.getvalue()

            # Load Postgres table to S3
            s3_client.put_object(Bucket=S3_BUCKET,
                        Key=S3_KEY,
                        Body=RAW_TABLE_ROWS_AS_STRING_VALUES
                        )
            root_logger.info(f'>>>>   Successfully written and loaded "{team_file}" file to the "{S3_BUCKET}" S3 bucket target location...')
            root_logger.debug(f'>>>>   ')
            root_logger.debug(f'------------------------------------------------------------------------------------------------- ')
            root_logger.debug(f'------------------------------------------------------------------------------------------------- ')
            root_logger.debug(f' ')
        
        else:
            df.to_csv(CSV_BUFFER , index=False)
            df.to_csv(f'{local_target_path}/{df}', index=False)
            root_logger.info("")
            root_logger.info(f'>>>>   Successfully written and loaded "{df}" file to local target location...')
            root_logger.debug(f'>>>>   ')



        # Add delays to avoid overloading the website's servers 
        sleep(3)

    except Exception as e:
        root_logger.error(e)




