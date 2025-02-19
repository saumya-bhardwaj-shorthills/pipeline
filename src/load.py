import pandas as pd
import os



class Loader:
    def __init__(self, file_path:str):
        self.file_path = file_path


    def load_data(self):
        try:
            for file in os.listdir(self.file_path):
                chunk = pd.read_csv("../data/" + file, sep="\t", header=None, dtype= str, usecols=[1,2,4], on_bad_lines="skip", compression="infer")
                yield chunk
        except Exception as error:
            raise error