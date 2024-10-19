from snowflake.snowpark import Session, DataFrame
from utils import create_snowpark_connection
from snowflake.snowpark.functions import col, when
from snowflake.snowpark.types import IntegerType, StringType


#region snowparkconnection
# create the snowpark session
session = create_snowpark_connection()

# get our raw dataframes from the snowflake stage
raw_data = session.read.table("JR_RAW.RAW_programming_languages")
raw_nationalities = session.read.table("JR_RAW.RAW_CREATORS_NATIONALITIES")
raw_github_issues = session.read.table('JR_RAW.RAW_GITHUB_ISSUES_BY_DATE')
raw_github_prs = session.read.table('JR_RAW.RAW_GITHUB_PRS_BY_DATE')
raw_github_repos = session.read.table('JR_RAW.RAW_GITHUB_TOTAL_REPOS')

raw_data.printSchema()
raw_nationalities.printSchema()
raw_github_issues.printSchema()
raw_github_prs.printSchema()
raw_github_repos.printSchema()
#endregion

#region check for duplicates in our snowflake dataframes
#function to check the duplicates on a dataFrame
def duplicate_count(data_frame: DataFrame) -> int:
    counter = data_frame.count() - data_frame.dropDuplicates().count()
    return counter
# check for duplicates 
base_duplicates = duplicate_count(raw_data)
print(f"Number of duplicates in base: {base_duplicates}")

natinality_duplicates = duplicate_count(raw_nationalities)
print(f"Number of duplicates in nationalities: {base_duplicates}")

github_issues_duplicates = duplicate_count(raw_github_issues)
print(f"Number of duplicates in github issues: {github_issues_duplicates}")

github_prs_duplicates = duplicate_count(raw_github_prs)
print(f"Number of duplicates in github prs: {github_prs_duplicates}")

github_repos_duplicates = duplicate_count(raw_github_repos)
print(f"Number of duplicates in github prs: {github_repos_duplicates}")
#endregion

#region handling Missing Values
# List of colums to check for null values
def check_for_null_and_0_values(raw_data) -> None:
    columns = raw_data.columns

    for column in columns:
        null_count = raw_data.filter(col(column).isNull()).count()
        na_count = raw_data.filter(col(column).isin("NA", "N/A", "NULL", "NaN", "")).count()
        print(f"Column: {column} has {null_count} null values and {na_count} NA values")

def check_df_0_values(raw_data):
    columns = raw_data.columns

    # check for 0 values
    for column in columns:
        zero_count = 0
        zero_count = raw_data.filter(col(column) == '0').count()
        print(f"Column: {column} has {zero_count} zero values")

"""
With this block code we can know if there is actually 0 or NA in our dataframes.

print("-------------------------raw_data 0 values and NA-------------------------")
check_df_0_values(raw_data)
check_for_null_and_0_values(raw_data)

print("-------------------------raw_nationalities 0 values and NA-------------------------")
check_df_0_values(raw_nationalities)
check_for_null_and_0_values(raw_nationalities)

print("-------------------------raw_github_issues 0 values and NA-------------------------")
check_df_0_values(raw_github_issues)
check_for_null_and_0_values(raw_github_issues)

print("-------------------------raw_github_prs 0 values and NA-------------------------")
check_df_0_values(raw_github_prs)
check_for_null_and_0_values(raw_github_prs)

print("-------------------------raw_github_repos 0 values and NA-------------------------")
check_df_0_values(raw_github_repos)
check_for_null_and_0_values(raw_github_repos)
"""

#set the only null values in IN_OPEN_SOURCE to NA
raw_data = raw_data.fillna({'is_open_source': "NA"})
#endregion

#region drop useless columns
"""
 From the above output we can see there is a few colums with null values and NA values, and we will drop the ones that are not useful.
 For exaple: Description, website, domain_name, domain_name_registered, reference, ISBNDB, BOOK_COUNT, semantic_scholar, github_repo_forks, github_repo_updated, github_repo_subscribers,
 github_repo_description...
"""
columns_to_drop = ["DESCRIPTION", "WEBSITE", "DOMAIN_NAME", "DOMAIN_NAME_REGISTERED", "REFERENCE", "ISBNDB", "BOOK_COUNT", "SEMANTIC_SCHOLAR", "CENTRAL_PACKAGE_REPOSITORY_COUNT", "GITHUB_REPO_UPDATED", "GITHUB_REPO_DESCRIPTION",
                            "GITHUB_REPO_FIRST_COMMIT", "GITHUB_LANGUAGE_TM_SCOPE", "GITHUB_LANGUAGE_ACE_MODE",
                            "GITHUB_LANGUAGE_FILE_EXTENSIONS", "WIKIPEDIA_SUMMARY", "WIKIPEDIA_SUMMARY", "WIKIPEDIA_PAGE_ID",
                            "WIKIPEDIA_RELATED", "LAST_ACTIVITY", "CENTRAL_PACKAGE_REPOSITORY_COUNT"
                   ]

raw_data = raw_data.drop(*columns_to_drop)

print("-----------------New schema after dropping columns-----------------")
raw_data.printSchema()
#endregion

#region create general dataframe
"""
Our objective is to make more manageable the original dataframe info by dividing them in a few vies.

The general dataframe is compose by the columns ["PLDB_ID","TITLE","TYPE","APPEARED","CREATORS",
                                          "CREATORS","LANGUAGE_RANK","FEATURES_HAS_COMMENTS",
                                          "FEATURES_HAS_SEMANTIC_INDENTATION","FEATURES_HAS_LINE_COMMENTS",
                                          "LINE_COMMENT_TOKEN","NUMBER_OF_USERS","NUMBER_OF_JOBS","ORIGIN_COMMUNITY"
                                          "FILE_TYPE","IS_OPEN_SOURCE"]
"""
columns_to_select = [
    "PLDB_ID", "TITLE", "TYPE", "APPEARED", "CREATORS",
    "LANGUAGE_RANK", "FEATURES_HAS_COMMENTS",
    "FEATURES_HAS_SEMANTIC_INDENTATION", "FEATURES_HAS_LINE_COMMENTS",
    "LINE_COMMENT_TOKEN", "NUMBER_OF_USERS", "NUMBER_OF_JOBS",
    "ORIGIN_COMMUNITY", "FILE_TYPE", "IS_OPEN_SOURCE"
]

# Select the specified columns from the raw_data DataFrame
general_dataFrame = raw_data.select(*columns_to_select)

#show the general dataframe
print('-----------------General Dataframe schema-----------------')
general_dataFrame.printSchema()
#endregion
            
#region create dataframe for github data 
""" 
This Dataframe will contain the title and all info related to github.
for this dataframe the columns we care about are ["TITLE", "GITHUB_REPO","GITHUB_REPO_STARS",
                                                      "GITHUB_REPO_FORKS","GITHUB_REPO_SUBSCRIBERS",
                                                      "GITHUB_REPO_CREATED","GITHUB_REPO_ISSUES",
                                                      "GITHUB_LANGUAGE","GITHUB_LANGUAGE_TYPE",
                                                      "GITHUB_LANGUAGE_REPOS"]
"""
columns_to_select = [
    "TITLE", "GITHUB_REPO","GITHUB_REPO_STARS",
    "GITHUB_REPO_FORKS","GITHUB_REPO_SUBSCRIBERS",
    "GITHUB_REPO_CREATED","GITHUB_REPO_ISSUES",
    "GITHUB_LANGUAGE","GITHUB_LANGUAGE_TYPE",
    "GITHUB_LANGUAGE_REPOS"]

# Select the specified columns from the raw_data DataFrame
github_dataFrame = raw_data.select(*columns_to_select)

#show the general dataframe
print('-----------------Github Datafram Schema-----------------')
github_dataFrame.printSchema()
#endregion

#region create wikipedia dataframe
"""
for this dataframe the columns we want are [
                                            "TITLE", "WIKIPEDIA","WIKIPEDIA_DAILY_PAGE_VIEWS",  
                                            "WIKIPEDIA_BACKLINKS_COUNT","WIKIPEDIA_APPEARED",
                                            "WIKIPEDIA_CREATED","WIKIPEDIA_REVISION_COUNT"  
                                            ]
"""
columns_to_select = [
                                "TITLE", "WIKIPEDIA","WIKIPEDIA_DAILY_PAGE_VIEWS",  
                                "WIKIPEDIA_BACKLINKS_COUNT","WIKIPEDIA_APPEARED",
                                "WIKIPEDIA_CREATED","WIKIPEDIA_REVISION_COUNT"  
                                ]

# Select the specified columns from the raw_data DataFrame
wikipedia_dataFrame = raw_data.select(*columns_to_select)

#show the general dataframe
print("-----------------Wikipedia Datafram schema-----------------")
wikipedia_dataFrame.printSchema()
#endregion

#region standardizing values for casting
# Function to replace "NA" for specific columns with a given value
def replace_na_value(data_frame, columns, value):
    # Create a copy of the DataFrame to avoid modifying the original
    clean_data_frame = data_frame

    # Prepare a list to hold updated columns
    updated_columns = []

    # Replace "NA" in the specified columns with the given value
    for column in columns:
        if column in clean_data_frame.columns:
            updated_columns.append(
                when(col(column) == "NA", value).otherwise(col(column)).alias(column)
            )
        else:
            print(f"Warning: Column '{column}' does not exist in the DataFrame.")
    
    # If there are any updated columns, select them along with the others
    if updated_columns:
        clean_data_frame = clean_data_frame.select(
            *updated_columns,
            *[col(c) for c in clean_data_frame.columns if c not in columns]
        )

    return clean_data_frame

#iterating over github Dataframe
git_columns  = [
    "GITHUB_REPO_STARS","GITHUB_REPO_FORKS",
    "GITHUB_REPO_SUBSCRIBERS","GITHUB_REPO_ISSUES"
]

github_data = replace_na_value(github_dataFrame, git_columns, "0")

print("-----------------Here we can see if in github some columns where NA was chanfed for 0-----------------")
github_data.show()

wiki_columns = [
    "WIKIPEDIA_DAILY_PAGE_VIEWS",  
    "WIKIPEDIA_BACKLINKS_COUNT",
    "WIKIPEDIA_REVISION_COUNT"
] 

wiki_data = replace_na_value(wikipedia_dataFrame, wiki_columns, "0")

wiki_data.show()
#endregion

#region cast into integer
def cast_column_into_integer(data_frame:DataFrame, columns_list:list):
    data_frame_casted = data_frame
    for column in columns_list:
        if  isinstance(column, str):
            data_frame_casted = data_frame_casted.with_column(column, col(column).cast(IntegerType()))
    return data_frame_casted
    
# cast columns from wikipedia table to integer
columns_to_cast = ['WIKIPEDIA_DAILY_PAGE_VIEWS','WIKIPEDIA_BACKLINKS_COUNT','WIKIPEDIA_REVISION_COUNT']
wiki_data = cast_column_into_integer(wiki_data, columns_to_cast)

# cast columns from github table to integer
columns_to_cast = ["GITHUB_REPO_STARS", "GITHUB_REPO_FORKS", "GITHUB_REPO_SUBSCRIBERS", "GITHUB_REPO_ISSUES"]
github_data = cast_column_into_integer(github_data, columns_to_cast)

#cast to integer the columns of the general table
columns_to_cast = ['NUMBER_OF_USERS','NUMBER_OF_JOBS']
general_dataFrame = cast_column_into_integer(general_dataFrame, columns_to_cast)

#cast to integer column in the nationality table
nationality_dataFrame = raw_nationalities.with_column('QUANTITY', col('QUANTITY').cast(IntegerType()))

#cast to integer the columns in the github issues table
columns_to_cast = ["COUNT","QUARTER"]
github_issues_dataFrame = cast_column_into_integer(raw_github_issues, columns_to_cast)

#cast to integer the columns in the github prs table
github_prs_dataFrame = cast_column_into_integer(raw_github_prs, columns_to_cast)

#cast to integer the columns in github repo table
columns_to_cast = ["NUM_REPOS"]
github_repo_dataFrame = cast_column_into_integer(raw_github_repos, columns_to_cast)
#endregion

#region change naming convetion for PK
github_data = github_data.withColumnRenamed("TITLE", "Language")
wiki_data = wiki_data.withColumnRenamed("TITLE", "Language")
general_dataFrame = general_dataFrame.withColumnRenamed("TITLE", "Language")
github_prs_dataFrame = github_prs_dataFrame.withColumnRenamed("NAME", "Language")
github_issues_dataFrame = github_issues_dataFrame.withColumnRenamed("NAME", "Language")
github_repo_dataFrame = github_repo_dataFrame.withColumnRenamed("LANGUAGE", "Language")
#endregion

wiki_data.printSchema()
github_data.printSchema()
general_dataFrame.printSchema()
github_repo_dataFrame.printSchema()
github_prs_dataFrame.printSchema()
github_issues_dataFrame.printSchema()

#send the dataframes to snowpark
github_data.write.save_as_table(table_name="JR_REFINED.REFINED_GITHUB_PROGRAMMING_LANGUAGES", mode="overwrite")
wiki_data.write.save_as_table(table_name="JR_REFINED.REFINED_WIKIPEDIA_PROGRAMMING_LANGUADES", mode= "overwrite")
general_dataFrame.write.save_as_table(table_name="JR_REFINED.REFINED_BASE_PROGRAMMING_LANGUAGES", mode="overwrite")
nationality_dataFrame.write.save_as_table(table_name="JR_REFINED.REFINED_CREATORS_NATIONALITIES", mode="overwrite")
github_repo_dataFrame.write.save_as_table(table_name="JR_REFINED.REFINED_GITHUB_TOTAL_REPOS", mode="overwrite")
github_prs_dataFrame.write.save_as_table(table_name="JR_REFINED.REFINED_GITHUB_PRS_BY_DATE", mode="overwrite")
github_issues_dataFrame.write.save_as_table(table_name="JR_REFINED.REFINED_GITHUB_ISSUES_BY_DATE", mode="overwrite")