from snowflake.snowpark import Session, DataFrame
from utils import create_snowpark_connection
from snowflake.snowpark.functions import col, when
from snowflake.snowpark.functions import sum as snow_sum
#region snowparkconnection
# create the snowpark session
session = create_snowpark_connection()
df_github = session.read.table("JR_REFINED.REFINED_GITHUB_PROGRAMMING_LANGUAGES")
df_wikipedia = session.read.table("JR_REFINED.REFINED_WIKIPEDIA_PROGRAMMING_LANGUADES")
df_general = session.read.table("JR_REFINED.REFINED_BASE_PROGRAMMING_LANGUAGES")
df_github_repos = session.read.table("JR_REFINED.REFINED_GITHUB_TOTAL_REPOS")
df_github_prs = session.read.table("JR_REFINED.REFINED_GITHUB_PRS_BY_DATE")
df_github_issues = session.read.table("JR_REFINED.REFINED_GITHUB_ISSUES_BY_DATE")
nationalities = session.read.table("JR_REFINED.REFINED_CREATORS_NATIONALITIES")

df_github.printSchema()
df_wikipedia.printSchema()
df_general.printSchema()
df_github_repos.printSchema()
df_github_prs.printSchema()
df_github_issues.printSchema()
nationalities.printSchema()
#endregion

#region for general analytics of activity data
github_joined = df_general.join(
    df_github,
    df_general["Language"] == df_github["Language"],
    "left"
).select(
    df_general["Language"].alias("Language"),
    df_general["Language_rank"],
    df_general["Appeared"],
    df_general["Creators"],
    df_general["Type"],
    df_general["Features_has_comments"],
    df_general["Features_has_semantic_indentation"],
    df_general["Features_has_line_comments"],
    df_general["Origin_community"],
    df_general["File_type"],
    df_general["Is_open_source"],
    df_general["Number_of_users"],
    df_general["Number_of_jobs"],
    df_github["github_repo_created"],
    df_github["github_repo_stars"],
    df_github["github_repo_forks"],
    df_github["github_repo_subscribers"]
    )
github_joined.printSchema()

final_activity_data = github_joined.join(
    df_wikipedia,
    github_joined["Language"] == df_wikipedia["Language"],
    "left"
).select(
    github_joined["Language"].alias("Language"),
    github_joined["Language_rank"],
    github_joined["Appeared"],
    github_joined["Creators"],
    github_joined["Type"],
    github_joined["Features_has_comments"],
    github_joined["Features_has_semantic_indentation"],
    github_joined["Features_has_line_comments"],
    github_joined["Origin_community"],
    github_joined["File_type"],
    github_joined["Is_open_source"],
    github_joined["Number_of_users"],
    github_joined["Number_of_jobs"],
    github_joined["github_repo_created"],
    github_joined["github_repo_stars"],
    github_joined["github_repo_forks"],
    github_joined["github_repo_subscribers"],
    df_wikipedia["WIKIPEDIA_CREATED"],
    df_wikipedia["WIKIPEDIA_DAILY_PAGE_VIEWS"],
    df_wikipedia["WIKIPEDIA_BACKLINKS_COUNT"],
    df_wikipedia["WIKIPEDIA_REVISION_COUNT"]
)
final_activity_data.printSchema()
#endregion


#region join nationalities to metrics
#join nationalities to language_rank, Number_of_users, Number_of_jobs, IS_OPEN_SOURCE, origin_community, github_repo_stars, github_repo_subscribers, WIKIPEDIA_DAILY_PAGE_VIEWS, WIKIPEDIA_REVISION_COUNT

nationalites_metrics = nationalities.join(
    final_activity_data,
    nationalities["Language"] == final_activity_data["Language"],
    "left"
).select(
    nationalities["Language"].alias("Language"),
    nationalities["Creators"].alias("Creators"),
    nationalities["Nationality"],
    nationalities["Quantity"],
    final_activity_data["Language_rank"],
    final_activity_data["Appeared"],
    final_activity_data["Type"],
    final_activity_data["Features_has_comments"],
    final_activity_data["Features_has_semantic_indentation"],
    final_activity_data["Features_has_line_comments"],
    final_activity_data["Origin_community"],
    final_activity_data["File_type"],
    final_activity_data["Is_open_source"],
    final_activity_data["Number_of_users"],
    final_activity_data["Number_of_jobs"],
    final_activity_data["github_repo_stars"],
    final_activity_data["github_repo_subscribers"],
    final_activity_data["WIKIPEDIA_DAILY_PAGE_VIEWS"],
    final_activity_data["WIKIPEDIA_REVISION_COUNT"]
)
nationalites_metrics.printSchema()
#endregion

#region send to snowflake
final_activity_data.create_or_replace_view("JR_CURATED.CURATED_GITHHUB_WIKI_ACTIVITY_DATA")
nationalites_metrics.create_or_replace_view("JR_CURATED.CURATED_NATIONALITIES_METRICS")
df_github_repos.create_or_replace_view("JR_CURATED.CURATED_GITHUB_REPOS")
df_github_prs.create_or_replace_view("JR_CURATED.CURATED_GITHUB_PRS_BY_DATE")
df_github_issues.create_or_replace_view("JR_CURATED.CURATED_GITHUB_ISSUES_BY_DATE")
#endregion