import pandas as pd

class Filter:
    def __init__(self, df:pd.DataFrame):
        self.df = df

    def find_rows_by_event_id(self, event_id: str)-> pd.DataFrame:
        df = self.df
        # lambda function splits the row elements and then filters with event_id,
        # apply method applies this function to each row in the series.
        event_id_filtered_df = df[df['post-event-list'].apply(lambda x: event_id in x.split(','))]
        return event_id_filtered_df


