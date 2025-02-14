import pandas as pd
import os



class Loader:
    def __init__(self, file_path:str):
        self.file_path = file_path


    def load_data(self):
        try:
            for file in os.listdir(self.file_path):
                for chunk in pd.read_csv("../data/" + file, sep="\t", header=None, dtype= str, on_bad_lines="skip", compression="infer", chunksize=100000):
                    yield chunk
        except Exception as error:
            raise error