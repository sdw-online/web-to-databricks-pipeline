import os
import pandas as pd
from pathlib import Path



api_caller_directory        =       os.path.abspath('api_caller/temp_storage')
scraper_directory           =       os.path.abspath('scraper/temp_storage')



for filename in os.listdir(scraper_directory):
    if filename.endswith(".csv"):
        file_path = os.path.join(scraper_directory, filename)
        df = pd.read_csv(file_path)
        df.dropna(axis=1, how='all', inplace=True)
        df = df.apply(lambda x: pd.Series(x.dropna().values), axis=1)
        df.to_csv(file_path, index=False)


        print(df)
        print("--------------------")
        print("")