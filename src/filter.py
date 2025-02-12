import pandas as pd

class Filter:
    def __init__(self, df:pd.DataFrame):
        self.df = df

    def find_rows_by_event_id(self, event_id: str)-> pd.DataFrame:
        try:
            df = self.df
            event_id_filtered_df = df[df['post-event-list'].apply(lambda x: isinstance(x, str) and event_id in x.split(','))]
            return event_id_filtered_df
        except Exception as error:
            print(error)


