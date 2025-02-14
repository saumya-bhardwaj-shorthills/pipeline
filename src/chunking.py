import pandas as pd
from filter import Filter
from transform import Transform
import traceback
import gc

class Chunk:
    def __init__(self, chunk_size: int):
        self.chunk_size = chunk_size
        self.chunk_list = []

    def process_data_in_chunks(self, event_id: str, chunk: pd.DataFrame):
        try:
            transform = Transform(chunk)
            transformed_data = transform.add_column_name()
            transformed_data = transform.drop_columns()
            filter = Filter(transformed_data)
            filtered_data = filter.find_rows_by_event_id(event_id)
            product_list = filter.segregate_product_list_item(filtered_data)
            new_df = filter.fetch_product_detail(product_list)
            self.chunk_list.append(new_df)
            del new_df
            gc.collect()
        except:
            pass
    
    def combine_df(self)-> pd.DataFrame:
        final_df = pd.concat(self.chunk_list)
        return final_df