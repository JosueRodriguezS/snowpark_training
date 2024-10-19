from snowflake.snowpark import Session, DataFrame
from utils import create_snowpark_connection
from snowflake.snowpark.functions import col, when
from snowflake.snowpark.functions import sum as snow_sum

#region snowparkconnection
# create the snowpark session
session = create_snowpark_connection()
df_github = session.read.table("JR_REFINED.REFINED_GITHUB_PROGRAMMING_LANGUAGES")
df_wikipedia = session.read.table("JR_REFINED.REFINED_WIKIPEDIA_PROGRAMMING_LANGUADES")
df_github.printSchema()
df_wikipedia.printSchema()
#endregion

