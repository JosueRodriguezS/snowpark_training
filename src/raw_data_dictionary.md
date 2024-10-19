pldb_id	                character	A standardized, uniquified version of the language name, used as an ID on the PLDB site.
title	                character	The official title of the language.
description	            character	Description of the repo on GitHub.
type	                character	Which category in PLDB's subjective ontology does this entity fit into.
appeared	            double	What year was the language publicly released and/or announced?
creators	            character	Name(s) of the original creators of the language delimited by " and "
website	                character	URL of the official homepage for the language project.
domain_name	            character	If the project website is on its own domain.
domain_name_registered	double	When was this domain first registered?
reference	            character	A link to more info about this entity.
isbndb	                double	Books about this language from ISBNdb.
book_count	            double	Computed; the number of books found for this language at isbndb.com
semantic_scholar	    integer	Papers about this language from Semantic Scholar.
language_rank	        double	Computed; A rank for the language, taking into account various online rankings. The computation for this column is not currently clear.
github_repo	            character	URL of the official GitHub repo for the project if it hosted there.
github_repo_stars	    double	How many stars of the repo?
github_repo_forks	    double	How many forks of the repo?
github_repo_updated	    double	What year was the last commit made?
github_repo_subscribers	    double	How many subscribers to the repo?
github_repo_created	        double	When was the Github repo for this entity created?
github_repo_description	    character	Description of the repo on GitHub.
github_repo_issues	        double	How many isses on the repo?
github_repo_first_commit	double	What year the first commit made in this git repo?
github_language	            character	GitHub has a set of supported languages as defined here
github_language_tm_scope	character	The TextMate scope that represents this programming language.
github_language_type	    character	Either data, programming, markup, prose, or nil.
github_language_ace_mode	character	A String name of the Ace Mode used for highlighting whenever a file is edited. This must match one of the filenames in http://git.io/3XO_Cg. Use "text" if a mode does not exist.
github_language_file_extensions	character	An Array of associated extensions (the first one is considered the primary extension, the others should be listed alphabetically).
github_language_repos	    double	How many repos for this language does GitHub report?
wikipedia	                character	URL of the entity on Wikipedia, if and only if it has a page dedicated to it.
wikipedia_daily_page_views	double	How many page views per day does this Wikipedia page get? Useful as a signal for rankings. Available via WP api.
wikipedia_backlinks_count	double	How many pages on WP link to this page?
wikipedia_summary	        character	What is the text summary of the language from the Wikipedia page?
wikipedia_page_id	        double	Waht is the internal ID for this entity on WP?
wikipedia_appeared	        double	When does Wikipedia claim this entity first appeared?
wikipedia_created	        double	When was the Wikipedia page for this entity created?
wikipedia_revision_count	double	How many revisions does this page have?
wikipedia_related	        character	What languages does Wikipedia have as related?
features_has_comments	logical	Does this language have a comment character?
features_has_semantic_indentation	logical	Does indentation have semantic meaning in this language?
features_has_line_comments	logical	Does this language support inline comments (as opposed to comments that must span an entire line)?
line_comment_token	        character	Defined as a token that can be placed anywhere on a line and starts a comment that cannot be stopped except by a line break character or end of file.
last_activity	                    double	Computed; The most recent of any year field in the PLDB for this language.
number_of_users	                    double	Computed; "Crude user estimate from a linear model.
number_of_jobs	                    double	Computed; The estimated number of job openings for programmers in this language.
origin_community	                character	In what community(ies) did the language first originate?
central_package_repository_count	double	Number of packages in a central repository. If this value is not known, it is set to 0 (so "0" can mean "no repository exists", "the repository exists but is empty" (unlikely), or "we do not know if a repository exists". This value is definitely incorrect for R.
file_type	                        character	What is the file encoding for programs in this language?
is_open_source	                    logical	Is it an open source project?