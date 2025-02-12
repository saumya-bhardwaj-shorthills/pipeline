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

    def segregate_product_list_item(self, event_filtered_df: pd.DataFrame)-> pd.DataFrame:
        # Segregate post-product-list based on post-event-list 
        event_filtered_df['post-product-list'] = event_filtered_df['post-product-list'].apply(lambda x: x.split(','))
        segregated_post_product_list_df = event_filtered_df.explode(column=["post-product-list"])
        return segregated_post_product_list_df