# Snowpark Project for Data Transformation and Sigma Views

## Overview
This project involves processing and transforming raw data from multiple CSV files into cleaned, structured data for further analysis and visualization. The data is sourced from publicly available datasets on Kaggle.

## Data Sources
The raw data primarily comes from the following Kaggle datasets:

- [Programming Languages Dataset by Sujay Kapadnis](https://www.kaggle.com/datasets/sujaykapadnis/programming-languages)
- [GitHub Programming Languages Dataset by Isaac Wen](https://www.kaggle.com/datasets/isaacwen/github-programming-languages-data)

These datasets contain various data related to programming languages, including usage statistics and trends over time.

## Purpose
The goal of this project is to clean, transform, and structure the raw data in Snowflake using Snowpark, and then present it via Sigma dashboards for easier analysis and decision-making.

## Workflow
1. **Data Ingestion**: The raw CSV files are loaded into Snowflake using Snowpark.
2. **Data Cleaning**: The data is cleaned and preprocessed to remove inconsistencies and ensure that it is in a suitable format for analysis.
3. **Data Transformation**: The cleaned data is transformed into meaningful views in Snowflake.
4. **Data Visualization**: The transformed data is then used to create customized visualizations and dashboards in Sigma.

## Setup
- **Snowflake**: Set up a Snowflake account and configure the necessary environments for loading and transforming the data.
- **Python**: Use Python and the Snowpark library to automate data ingestion, cleaning, and transformation tasks.
- **Sigma**: Create custom dashboards in Sigma to visualize the results.

## Conclusion
This project demonstrates the ability to work with Snowflake and Snowpark to process and visualize data for business insights, specifically in the context of programming language trends and GitHub data.
