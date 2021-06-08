### Data Engineering Capstone Project

#### Project Summary

The objective of this project was to create an ETL pipeline for I94 immigration, global land temperatures, US demographics and aiport datasets to form an analytics database on immigration events. This analytics database could be useful for usecases such as studying the effects of temperature on the volume of travellers, how seasonality affects travel, the differences between people travelling from different climates, etc. 

Following are the datasets being used to create the ETL pipeline:

[I94 Immigration Data](https://travel.trade.gov/research/reports/i94/historical/2016.html): This data comes from the US National Tourism and Trade Office and includes the contents of the i94 form on entry to the united states. A data dictionary is included in the workspace.

[World Temperature Data](https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data): This dataset comes from Kaggle and includes the temperatures of various cities in the world from 1743 to 2013.

[U.S. City Demographic Data](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export): This data comes from OpenSoft. It contains information about the demographics of all US cities and census-designated places with a population greater or equal to 65,000 and comes from the US Census Bureau's 2015 American Community Survey.

[Airport Codes](https://datahub.io/core/airport-codes#data): This is a simple table of airport codes and corresponding cities.

The project follows the follow steps:
* Scope the Project and Gather Data
* Explore and Assess the Data
* Define the Data Model
* Run ETL to Model the Data
* Complete Project Write Up

The scope of this project is to create an ETL pipeline from four different data sources, create fact and dimensional tables, and be able to do different sorts of analysis on US immigration using factors of city, temperature, demographics. 

####  Conceptual Data Model

The data model to be used for this ETL is the star schema. It is simple, effective, will process quickly, and the joins will be simpler. 

Following are the staging and final tables used in the schema.

#### Staging Tables:
| table name   |  columns             |
| ------------ | ----------------    | 
| airports     | airport_name, type, country, state, gps_code, local_code, coordinates |
| immigration  | id, date, city_code, state_code, age, gender, visa_type, count |
| temperature  | year, month, city_code, avg_temperature, lat, long | 
| demographics | city_code, state_code, city_name, median_age, pcs_male_pop, pct_female_pop, pct_veterans, pct_foreign_born, pct_native_american, pct_asian, pct_black, pct_hispanic_or_latino, pct_white, total_pop |

#### Final Tables:
| table name | columns | type |
|-------------|----------|-----------|
| immigration_df | id, state_code, city_code, date, count | fact|
| immigrant_df | id, gender, age, visa_type | dimensional |
| city_stat_df | city_code, state_code, city_name, median_age, pct_male_pop, pct_female_pop, pct_veterans, pct_foreign_born, pct_native_american, pct_asian, pct_black, pct_hispanic_or_latino, pct_white, total_pop, lat, long | dimensional |
| monthly_city_temp | city_code, year, month, avg_temperature | dimensional | 

#### Mapping Out Data Pipelines
The steps necessary to pipeline the data into star schema model:
- Clean the data of nulls, duplicates, fix data_types
- Load the staging tables
- Create fact and dimensional tables
- Save the processed data into parquet files for further uses

#### Data Quality Checks
The data has to go thropugh quality checks are performed to ensure the pipeline ran as expected. These could include:
 * Integrity constraints on the relational database (e.g., unique key, data type, etc.)
 * Unit tests for the scripts to ensure they are doing the right thing
 * Source/Count checks to ensure completeness