import pandas as pd
import os



class Loader:
    def __init__(self, file_path:str):
        self.file_path = file_path


    def load_data(self)-> pd.DataFrame:
        combined_df = pd.DataFrame()
        try:
            for file in os.listdir(self.file_path):
                temp_df = pd.read_csv("../data/" + file, sep="\t", header=None, dtype= str, on_bad_lines="skip", compression="infer")
                temp_df.fillna('', inplace=True)
                combined_df = pd.concat([combined_df, temp_df])
            return combined_df
        except Exception as error:
            raise error