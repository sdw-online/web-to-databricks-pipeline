import os
import pandas as pd
import shutil

import re

api_caller_directory                    =       os.path.abspath('api_caller/temp_storage')

original_scraped_data_directory         =       os.path.abspath('scraper/temp_storage/dirty_data')
dev_scraped_data_directory              =       os.path.abspath('scraper/temp_storage/clean_data')



def copy_and_paste_csv_files(source_directory, target_directory):
    print()
    print('>>> Copying CSV files from source and pasting to target directory.....')
    for filename in os.listdir(source_directory):
        if filename.endswith('.csv'):
            source_path = os.path.join(source_directory, filename)
            target_path = os.path.join(target_directory, filename)
            shutil.copy2(source_path, target_path)
            print(f"      >>> Successfully copied '{filename}' file from source to target destination ")
    
    print('>>> Copy and paste task completed...advancing to next task .....')
    print("--------------------")
    print()
    print()



def remove_blank_fields(directory):
    print('>>> Removing any blank fields from CSV files.....')
    for filename in os.listdir(directory):
        csv_filepath = os.path.join(directory, filename)
        if filename.endswith(".csv"):
            df = pd.read_csv(csv_filepath)
            df.dropna(axis=1, how='all', inplace=True)
            df = df.apply(lambda x: pd.Series(x.dropna().values), axis=1)
            df.to_csv(csv_filepath, index=False)
            # df.to_csv(f'{csv_filepath}.csv', index=False)
            # print('CSV: ')
            print(f'CSV - {filename}:  ')
            print(df)
            print("--------------------")
            print("")


def remove_consecutive_commas(directory):
    print('>>> Removing any consecutive commas from CSV files.....')
    for filename in os.listdir(directory):
        csv_filepath = os.path.join(directory, filename)
        with open(csv_filepath, 'r') as csv_file:
            data = csv_file.read()
        
        data = re.sub(f',+', ',', data)
        data = re.sub(r', ,', ',', data)
        with open(csv_filepath, 'w') as csv_file:
            csv_file.write(data)
        column_names = ["Pos", "Team", "P", "W", "D", "L", "GF", "GA", "W", "D", "L", "GF", "GA", "GD", "Pts", "match_date", "drop_this_field_1", "drop_this_field_2", "drop_this_field_3"]
        df = pd.read_csv(csv_filepath, sep=',', header=None)
        

        df = df.drop(df.index[0])

        df.columns = column_names 
        
         # Drop specific columns
        df = df.drop(columns=["drop_this_field_1", "drop_this_field_2", "drop_this_field_3"])

        # Save data frame as CSV file
        df.to_csv(csv_filepath, index=False)
    
        
        print(f'Data frame - {filename}:  ')
        print("")
        print(df)
        print("")
        print(f">>> Successfully saved '{filename}' file as CSV file to target destination. ")
        print("-----------------------------------------------------------------------------")
        print("")
        print("")


    print()
    print()
    print("----------------------------------------------------------")
    print('>>> CSV file transformations completed. Terminating session.')
    print("----------------------------------------------------------")
    print()
    print()


copy_and_paste_csv_files(original_scraped_data_directory, dev_scraped_data_directory)
remove_blank_fields(dev_scraped_data_directory)
remove_consecutive_commas(dev_scraped_data_directory)
