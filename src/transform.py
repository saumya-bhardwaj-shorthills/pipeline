import pandas as pd

class Transform:
    def __init__(self, df:pd.DataFrame):
        self.df = df

    def add_column_name(self)-> pd.DataFrame:
        self.df.columns = ["Date", "post-event-list", "post-product-list", "unknownA", "url", "unknownB"]
        self.df = self.df.astype(str) # Converting all columns to string beforing NAN, Because pandas does not allow replacement of NAN (float) with a string in numerical column
        self.df.fillna('', inplace=True)
        return self.df
    def drop_columns(self)-> pd.DataFrame:
        self.df.drop(["unknownA", "unknownB"], axis=1, inplace=True)
        return self.df
    
    