# Snowpark Project for Data Transformation and Sigma Views

This project uses Snowflake's **Snowpark** library to ingest, clean, and transform raw data from five CSV files. The cleaned and transformed data is stored in Snowflake and organized into views optimized for **Sigma** analytics.

## Project Overview

1. **Ingest**: Load raw data from five CSV files into Snowflake.
2. **Clean**: Perform data cleaning tasks such as handling null values, correcting data types, and standardizing fields.
3. **Transform**: Apply transformations to the cleaned data to prepare it for analysis.
4. **Create Views**: Generate views for Sigma to visualize and analyze the transformed data.

## Prerequisites

- **Snowflake Account**: Ensure you have access to a Snowflake environment.
- **SnowSQL**: Install SnowSQL to upload and interact with Snowflake.
- **Python**: Install Python (version 3.8 or higher) to run Snowpark scripts.
- **Snowpark Library**: Install the Snowpark Python library:
  ```bash
  pip install snowflake-snowpark
