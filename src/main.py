from load import Loader

def main():
    file_path = "../data"
    loader = Loader(file_path)
    try:
        result = loader.load_data()
        print(result)
    except Exception as error:
        print(f"Error occurred: {error}")

if __name__ == "__main__":
    main()
