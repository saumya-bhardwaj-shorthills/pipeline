from load import Loader
from transform import Transform
from filter import Filter


def main():
    file_path = "../data"
    event_id = "20113"
    loader = Loader(file_path)
    try:
        extracted_data = loader.load_data()
        transform = Transform(extracted_data)
        transformed_data = transform.add_column_name()
        transformed_data = transform.drop_columns()
        filter = Filter(transformed_data)
        filtered_data = filter.find_rows_by_event_id(event_id)
        print(filtered_data)
    except Exception as error:
        print(f"Error occurred: {error}")

if __name__ == "__main__":
    main()
