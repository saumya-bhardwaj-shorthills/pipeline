import pandas as pd
import re
import gc

class Filter:
    def __init__(self, df: pd.DataFrame, chunk_size=100000):
        self.df = df
        self.chunk_size = chunk_size  # Process data in chunks

    def find_rows_by_event_id(self, event_id: str) -> pd.DataFrame:
        df = self.df
        event_id_filtered_df = df[df['post-event-list'].apply(lambda x: event_id in x.split(','))]
        return event_id_filtered_df

    def segregate_product_list_item(self, event_filtered_df: pd.DataFrame) -> pd.DataFrame:
        event_filtered_df.loc[:, 'post-product-list'] = event_filtered_df['post-product-list'].apply(lambda x: x.split(','))
        segregated_post_product_list_df = event_filtered_df.explode(column=["post-product-list"])
        return segregated_post_product_list_df

    def process_chunk(self, chunk: pd.DataFrame) -> pd.DataFrame:        
        def extract_fields(row):
            parts = row.split(';')

            dealer_id = parts[0] if len(parts) > 0 else ""
            ad_id = parts[1] if len(parts) > 1 else ""

            event_data = parts[3] if len(parts) > 3 else ""
            unknown_data = parts[4] if len(parts) > 4 else ""

            impression_event_id = ""
            impression_count = 0 

            if event_data:
                for event in event_data.split('|'):
                    event_parts = event.split('=')
                    if len(event_parts) == 2:
                        impression_event_id = event_parts[0]
                        try:
                            impression_count = int(event_parts[1])
                        except ValueError:
                            impression_count = 0
                        break

            return dealer_id, ad_id, impression_event_id, impression_count, unknown_data

        chunk[['dealer_id', 'ad_id', 'impression_event_id', 'impression_count', 'unknown_data']] = (
            chunk['post-product-list'].map(lambda x: extract_fields(str(x))).apply(pd.Series)
        )

        chunk.drop(columns=['post-product-list'], inplace=True)
        gc.collect()

        def extract_website(url):
            match = re.search(r'https?://(?:www\.)?([\w\-]+\.\w+)', str(url))
            return match.group(1) if match else ""

        chunk['website'] = chunk['url'].map(extract_website)

        chunk.drop(columns=['url'], inplace=True)
        gc.collect()

        chunk['impression_count'] = chunk['impression_count'].astype(int)

        return chunk[['dealer_id', 'ad_id', 'impression_event_id', 'impression_count', 'unknown_data', 'website']]

    def fetch_product_detail(self, dataframe) -> pd.DataFrame:
        result_chunks = []

        for i in range(0, len(dataframe), self.chunk_size):
            chunk = dataframe.iloc[i:i+self.chunk_size].copy()
            processed_chunk = self.process_chunk(chunk)
            result_chunks.append(processed_chunk)

            del chunk, processed_chunk
            gc.collect()
        final_df = pd.concat(result_chunks, ignore_index=True)
        return final_df
