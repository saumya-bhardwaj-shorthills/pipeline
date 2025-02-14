from load import Loader
from chunking import Chunk

def main():
    file_path = "../data"
    event_id = "20113"
    chunk_size = 100000
    loader = Loader(file_path)
    chunking = Chunk(chunk_size)
    try:
        for chunk in loader.load_data():
            chunking.process_data_in_chunks(event_id, chunk)
    except Exception as error:
        print(f"Error occurred: {error}")
    final_df = chunking.combine_df()
    print(final_df)

if __name__ == "__main__":
    main()
