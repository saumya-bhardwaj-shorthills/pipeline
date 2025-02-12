import pandas as pd

class Transform:
    def __init__(self, df:pd.DataFrame):
        self.df = df

    def add_column_name(self)-> pd.DataFrame:
        self.df.columns = ["Date", "post-event-list", "post-product-list", "unknownA", "url", "unknownB"]
        return self.df
    def drop_columns(self)-> pd.DataFrame:
        self.df.drop(["unknownA", "unknownB"], axis=1, inplace=True)
        return self.df
    