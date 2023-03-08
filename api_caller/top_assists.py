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

local_target_path               =       os.path.abspath('api_caller/temp_storage/top_assists/dirty_data')
match_dates                     =       ['2022-09-01', '2022-10-01', '2022-11-01', '2022-12-01', '2023-01-01', '2023-02-01', '2023-03-01', '2023-03-08']
headers                         =       {"X-RapidAPI-Key": API_KEY, "X-RapidAPI-Host": API_HOST}
query_string                    =       {'league': league_id, 'season': season}


  
try:

    # Send HTTP request for football data to Rapid-API endpoint
    top_assists_url                                 =       f'https://api-football-v1.p.rapidapi.com/v3/players/topassists'
    top_assists_file                                =       f'top_assists.csv'
    S3_KEY                                          =       S3_FOLDER + top_assists_file
    CSV_BUFFER                                      =       io.StringIO()

    
    root_logger.info(f'>>>>   Sending HTTP GET requests to API endpoint for latest top_assists in the Premier League ...')
    root_logger.debug(f'>>>>   ')
    response                        =       requests.request("GET", top_assists_url, headers=headers, params=query_string)
    # root_logger.debug(response.text)

    # Display the response in a readable JSON format
    root_logger.info(f'>>>>   Requests completed, now processing JSON payload ...')
    root_logger.debug(f'>>>>   ')
    response_json = json.dumps(response.json(), indent=4)
    # root_logger.debug(response_json)


    # Read JSON payload into data frame 
    root_logger.info(f'>>>>   Reading JSON payload into data frame ...')
    root_logger.debug(f'>>>>   ')
    
    players                     =   []
    statistics                  =   []

    player_loop_counter         =   0
    statistics_loop_counter     =   0


    
    for index in range(20):

        player_loop_counter += 1
        player_details = response.json()['response'][index]['player']
        players.append(player_details)
        root_logger.debug(f"Player count: {player_loop_counter} ")

        statistics_loop_counter += 1
        player_statistics = response.json()['response'][index]['statistics']
        statistics.append(player_statistics)
        root_logger.debug(f"Statistics count: {statistics_loop_counter} ")

    player_details_df = pd.DataFrame(players)
    player_statistics_df = pd.DataFrame(statistics)


    top_assists_df = pd.concat([player_details_df, player_statistics_df], axis=1)

    print('------------')
    print('')
    print(top_assists_df)
    print('')
    print('------------')

    # Write data frame to CSV file
    root_logger.info(f'>>>>   Writing data frame to CSV file ...')
    root_logger.debug(f'>>>>   ')




    if WRITE_TO_CLOUD:
        top_assists_df.to_csv(CSV_BUFFER , index=False)
        RAW_TABLE_ROWS_AS_STRING_VALUES              =       CSV_BUFFER.getvalue()

        # Load Postgres table to S3
        s3_client.put_object(Bucket=S3_BUCKET,
                    Key=S3_KEY,
                    Body=RAW_TABLE_ROWS_AS_STRING_VALUES
                    )
        root_logger.info(f'>>>>   Successfully written and loaded "{top_assists_file}" file to the "{S3_BUCKET}" S3 bucket target location...')
        root_logger.debug(f'>>>>   ')
        root_logger.debug(f'------------------------------------------------------------------------------------------------- ')
        root_logger.debug(f'------------------------------------------------------------------------------------------------- ')
        root_logger.debug(f' ')
    
    else:
        top_assists_df.to_csv(f'{local_target_path}/{top_assists_file}', index=False, encoding='utf-8')
        root_logger.info("")
        root_logger.info(f'>>>>   Successfully written and loaded "{top_assists_file}" file to local target location...')
        root_logger.debug(f'>>>>   ')



    # Add delays to avoid overloading the website's servers 
    sleep(3)

except Exception as e:
    root_logger.error(e)




