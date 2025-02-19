import pandas as pd

class Group:
    def __init__(self, df: pd.DataFrame):
        self.df = df

    def string_conversion(self, id: str)-> str:
        return id.isdigit()
    
    def group_by(self, group_columns: list[str], target_column: str)-> pd.DataFrame:
        self.df["Impression_count"] = pd.to_numeric(self.df["Impression_count"])
        self.df["dealer_id"] = self.df["dealer_id"].apply(lambda x: x if self.string_conversion(x) else "0")
        self.df["dealer_id"] = pd.to_numeric(self.df["dealer_id"])
        grouped_df = self.df.groupby(group_columns)[target_column].sum().reset_index()
        return grouped_df