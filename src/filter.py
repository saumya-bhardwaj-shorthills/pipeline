import pandas as pd
import traceback

class Filter:
    def __init__(self, df: pd.DataFrame, chunk_size=100000):
        self.df = df
        self.chunk_size = chunk_size  # Process data in chunks
        self.product_lsit = []

    def find_rows_by_event_id(self, event_id: str) -> pd.DataFrame:
        df = self.df
        event_id_filtered_df = df[df['post-event-list'].apply(lambda x: event_id in x.split(','))]
        return event_id_filtered_df

    def segregate_product_list_item(self, event_filtered_df: pd.DataFrame) -> pd.DataFrame:
        event_filtered_df.loc[:, 'post-product-list'] = event_filtered_df['post-product-list'].apply(lambda x: x.split(','))
        segregated_post_product_list_df = event_filtered_df.explode(column=["post-product-list"])
        return segregated_post_product_list_df

    def fetch_product_detail(self, chunk: pd.DataFrame)->pd.DataFrame:
        chunk.loc[:, 'post-product-list'] = chunk['post-product-list'].apply(lambda x: x.split(';'))
        chunk.apply(lambda x : self.extract_fields(x['post-product-list'], x['url']), axis=1)

        product_info = pd.DataFrame(self.product_lsit)

        return product_info
        

    def extract_fields(self, product_arr, url):

        product_dict = {
            "dealer_id": '',
            "Ad_id" : '',
            "Impression_count": '',
            "Website": '', 
            "other_data": ''
        }
        try:
            if len(product_arr) > 0 and product_arr[0]:
                product_dict['dealer_id'] = product_arr[0]
            else:
                pass
            if len(product_arr) > 1 and product_arr[1]:
                product_dict['Ad_id'] = product_arr[1]
            else:
                pass
            if len(product_arr) > 4 and product_arr[4] and '|' in product_arr[4]:
                product_arr_info = product_arr[4].split('|')
                if len(product_arr_info) > 0 and product_arr_info[0] and '=' in product_arr_info[0]:
                    product_arr_info_impression_count = product_arr_info[0].split('=')
                    if product_arr_info_impression_count[-1]:
                        product_dict['Impression_count'] = product_arr_info_impression_count[-1]
                    else:
                        pass
                else:
                    pass
            else:
                pass
            if url and '/' in url:
                parsed_url = url.split('/')
                if parsed_url[2] and '.' in parsed_url[2]:
                    website_url = parsed_url[2].split('.')
                    if website_url[1]:
                        product_dict['Website'] = website_url[1]
                    else:
                        pass
                else:
                    pass
            self.product_lsit.append(product_dict)
        except:
            traceback.print_exc()