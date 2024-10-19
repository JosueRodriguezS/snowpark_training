import snowflake.snowpark as snowpark
import json
import os

def read_creds_from_json() -> dict:

    script_dir: str = os.path.dirname(os.path.abspath(__file__))
    creds_path: str = os.path.join(script_dir, 'creds.json')

    if not os.path.exists(creds_path):
        raise ValueError('No creds.json file found.')
    try:
        with open(creds_path) as f:
            connection_parameters = json.load(f)
    except Exception as e:
        raise ValueError('Error reading JSON file.') from e
    return connection_parameters


def create_snowpark_connection():
    creds = read_creds_from_json()
    return snowpark.Session.builder.configs(creds).create()  

def test_connection():
    try:
        session = create_snowpark_connection()
        
        # Check the connection by executing a simple query
        df = session.sql("SELECT CURRENT_VERSION()").to_pandas()
        
        print(f"Snowflake version: {df.iloc[0, 0]}")
        
        session.close()
    
    except Exception as e:
        print(f"An error occurred: {e}")


# Run the test function
test_connection()