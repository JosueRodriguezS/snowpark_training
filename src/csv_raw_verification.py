
import pandas as pd
import dotenv
import os

dotenv.load_dotenv()

#region csv verification
"""
Function to verify the contents of the csv file using pandas since snowpark does not support reading csv files directly. 
To clarify, for using snowpark, the csv file must be loaded into a stage in snowflake and then read into a DataFrame.

"""
def verify_csv_content(file_path: str) -> pd.DataFrame:
    try:
        # read the csv file using pandas
        df = pd.read_csv(file_path)

        #verify dimensions
        rows, cols = df.shape
        print(f"Rows: {rows}, Columns: {cols}")

        #verify header
        print(df.columns)
        print(len(df.columns))

        return df
    except Exception as e:
        print(f"An error occurred related to the csv file: {e}")


verify_csv_content(os.getenv('CSV_FILE_PATH'))