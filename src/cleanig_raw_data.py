from snowflake.snowpark import Session
import logging
from utils import create_snowpark_connection
from snowflake.snowpark.functions import col

# create the snowpark session
session = create_snowpark_connection()

# get our data frame from the snoflake stage
raw_data = session.read.table("RAW_programming_languages")

raw_data.printSchema()

# show the first 5 rows of the data
raw_data.show(5)

# show descriptive statistics
raw_data.describe().show()

# check for duplacates 
duplicates = raw_data.count() - raw_data.dropDuplicates().count()
print(f"Number of duplicates: {duplicates}")

# List of colums to check for null values
columns = raw_data.columns

for column in columns:
    null_count = raw_data.filter(col(column).isNull()).count()
    na_count = raw_data.filter(col(column).isin("NA", "N/A", "NULL", "NaN", "")).count()
    print(f"Column: {column} has {null_count} null values and {na_count} NA values")

#set the only null values in IN_OPEN_SOURCE to NA
raw_data = raw_data.fillna({'is_open_source': "NA"})

# check for 0 values
for column in columns:
    zero_count = 0
    zero_count = raw_data.filter(col(column) == '0').count()
    print(f"Column: {column} has {zero_count} zero values")

"""
 From the above output we can see there is a few colums with null values and NA values, and we will drop the ones that are not useful.
 For exaple: Description, website, domain_name, domain_name_registered, reference, ISBNDB, BOOK_COUNT, semantic_scholar, github_repo_forks, github_repo_updated, github_repo_subscribers,
 github_repo_description, github_repo_issues, github_repo_first_commit, github_language, github_language_tm_scope, github_language_type, github_language_ace_mode,
 github_language_file_extensions, github_language_repos, wikipedia_backlinks_count, wikipedia_summary, wikipedia_page_id, wikipedia_revision_count, wikipedia_related, 
 central_package_repository_count 
"""
columns_to_drop = ["Description", "website", "domain_name", "domain_name_registered", "reference", "ISBNDB", "BOOK_COUNT", "semantic_scholar", "github_repo_forks", "github_repo_updated", "github_repo_subscribers",
                    "github_repo_description", "github_repo_issues", "github_repo_first_commit", "github_language", "github_language_tm_scope", "github_language_type", "github_language_ace_mode",
                    "github_language_file_extensions", "github_language_repos", "wikipedia_backlinks_count", "wikipedia_summary", "wikipedia_page_id", "wikipedia_revision_count", "wikipedia_related", 
                    "central_package_repository_count"]

raw_data = raw_data.drop(*columns_to_drop)

print("New schema after dropping columns")
raw_data.printSchema()