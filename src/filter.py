import pandas as pd
import re

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
        event_filtered_df.loc[:, 'post-product-list'] = event_filtered_df['post-product-list'].apply(lambda x: x.split(','))
        segregated_post_product_list_df = event_filtered_df.explode(column=["post-product-list"])
        return segregated_post_product_list_df
    
    def fetch_product_detail(self, product_list: pd.DataFrame) -> pd.DataFrame:
        def extract_fields(row):
            try:
                parts = row.split(';')
                dealer_id = parts[0] if len(parts) > 0 else ""
                ad_id = parts[1] if len(parts) > 1 else ""
                event_data = parts[3] if len(parts) > 3 else ""
                unknown_data = parts[4] if len(parts) > 4 else ""
                impression_event_id = ""
                impression_count = "0"
                if event_data:
                    events = event_data.split('|')
                    for event in events:
                        event_parts = event.split('=')
                        if len(event_parts) == 2:
                            impression_event_id = event_parts[0] 
                            impression_count = event_parts[1]  
                            break  
                return dealer_id, ad_id, impression_event_id, impression_count, unknown_data

            except Exception as e:
                print(f"Error processing row: {row} -> {e}")
                return "", "", "", "0", ""
        product_list[['dealer_id', 'ad_id', 'impression_event_id', 'impression_count', 'unknown_data']] = product_list['post-product-list'].apply(
            lambda x: pd.Series(extract_fields(str(x)))
        )
        def extract_website(url):
            match = re.findall(r'https?://(?:www\.)?([\w\-]+\.\w+)', str(url))
            return match[0] if match else ""
        product_list['website'] = product_list['url'].apply(extract_website)

        new_df = product_list[['dealer_id', 'ad_id', 'impression_event_id', 'impression_count', 'unknown_data', 'website']]

        return new_df
