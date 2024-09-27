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
        raise ValueError(f'Error reading JSON file.') from e
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

raw_header = '''pldb_id,title,description,type,appeared,creators,website,domain_name,domain_name_registered,reference,isbndb,book_count,semantic_scholar,language_rank,github_repo,github_repo_stars,github_repo_forks,github_repo_updated,github_repo_subscribers,github_repo_created,github_repo_description,github_repo_issues,github_repo_first_commit,github_language,github_language_tm_scope,github_language_type,github_language_ace_mode,github_language_file_extensions,github_language_repos,wikipedia,wikipedia_daily_page_views,wikipedia_backlinks_count,wikipedia_summary,wikipedia_page_id,wikipedia_appeared,wikipedia_created,wikipedia_revision_count,wikipedia_related,features_has_comments,features_has_semantic_indentation,features_has_line_comments,line_comment_token,last_activity,number_of_users,number_of_jobs,origin_community,central_package_repository_count,file_type,is_open_source'''

my_header = raw_header.split(',')

print(len(my_header))