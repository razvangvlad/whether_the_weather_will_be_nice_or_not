# Weather Data Pipeline

## Table of Contents
1. [Overview](#overview)
2. [Project Structure](#project-structure)
3. [DBT Weather Data Model](#dbt-weather-data-model)
   - [Processing Layers](#processing-layers)
   - [Project Structure (DBT)](#project-structure-dbt)
4. [Airflow DAG](#airflow-dag)
   - [Weather_ETL_DAG](#weather_etl_dag)
5. [Python Scripts](#python-scripts)
6. [Setup Instructions](#setup-instructions)
7. [Conclusion](#conclusion)





## Overview
This project demonstrates a robust data pipeline using Apache Airflow and dbt to fetch, transform, and load weather data from the Meteomatics API into a Snowflake data warehouse. The pipeline follows dimensional modeling principles, specifically the star schema, to organize and optimize the data for analysis.


### Key Features
- **ETL Configuration**: The ETL flow can be configured to fetch data for the last 48 hours (API limitation) and for a range of locations.
- **Default Settings**: The initial load fetches the last 25 hours of data from the API for Aarhus, Amsterdam, and Berlin by default. A limit of 48 hours is set by the API with the current free plan. After the initial load, it is configured to be run at any time and provide the missing data from the last import by retrieving the latest date in the D_DATE dimension table.
- **Customization**: Further customization is possible by adjusting specific parameters in configuration files (more details can be found below).
- **Logging**: The logging is done at the application level (Airflow/DBT/Python_module)


### Configuration Details
#### `configuration_parameters.csv`
- **default_time_interval_hours**: Set to 24 by default, this parameter defines the size of the initial load.
- **start_datetime_UTC**: Set to `2024-06-12 04:00:00` but inactivated (ACTIVE column set to No). When activated (set to Yes), it can be used to load data for a specific time interval.
- **end_datetime_UTC**: Set to `2024-06-12 06:00:00` but inactivated (ACTIVE column set to No). When activated (set to Yes), it can be used to load data for a specific time interval.

#### `location_parameters.csv`
- **Location Customization**: Add any city in the world by inserting the CITY, COUNTRY, LATITUDE, and LONGITUDE, and set the ACTIVE flag to Yes.
- **Enable/Disable Locations**: Toggle the ACTIVE flag to Yes/No to add or remove locations from the ETL process.

#### `data_parameters.csv`
- **Data Parameters**: Configure specific data fetching parameters. This section is currently limited by implementation and API constraints.

By using these configuration files, you can easily customize the ETL process to meet your specific needs, whether it's adjusting the time interval for data fetching or adding new locations to monitor.


## Project structure
 - dbt Models: Structures and transforms the weather data.
 - Airflow DAG: Orchestrates the ETL process.
 - Python Scripts: Handle configuration, data fetching, and utility functions.
 - Configuration Files: Define settings and sources for the ETL process.
 - SQL Schema Files: Create tables in the Database.





# DBT Weather Data Model

## Overview
This dbt project is designed to manage and transform weather data. The project includes various models to organize and process weather-related information. This README provides an overview of the project structure and details on each model included in the project.

## Processing layers
1. staging
    - data is pushed to the WEATHER_RAW table using the Python module; truncated at the end of the ETL flow.
2. pstaging
    - data is inserted into the WEATHER_RAW_P persistent table incrementally based on a surrogate key hash created from (LATITUDE,LONGITUDE,DATE) columns
3. marts
    - data is first inserted into the dimensions where each dimension has a surrogate key.
    - data is push to the F_WEATHER_DATA fact table incrementally based on the surrogate key hash created from (LATITUDE,LONGITUDE,DATE) columns

## Project Structure
```plaintext
dbt_project:
|--dbt_project.yml
|--packages.yml
|--dbt_packages
|  |--dbt_utils
|--models
|  |--staging
|  |  |--__sources.yml
|  |--pstaging
|  |  |--weather_raw_p.sql
|  |  |--weather_raw_p.yml
|  |--marts
|  |  |--d_date.sql
|  |  |--d_date.yml
|  |  |--d_location.sql
|  |  |--d_location.yml
|  |  |--d_unit.sql
|  |  |--d_unit.yml
|  |  |--d_weather_condition.sql
|  |  |--d_weather_condition.yml
|  |  |--f_weather_data.sql
|  |  |--f_weather_data.yml
|--seeds
|  |--configurations
|  |  |--configuration_parameters.csv
|  |  |--data_parameters.csv
|  |  |--location_parameters.csv
|--nomenclatures
|  |  |--weather_conditions.csv
```




# Airflow
The Airflow DAG orchestrates the ETL (Extract, Transform, Load) process for weather data, ensuring data consistency and proper sequence of operations. 

## Weather_ETL_DAG
The flow is split into two task groups based on a branching component that checks whether the previous ETL flow failed using the **check_task** BranchPythonOperator:

1.  **previous_processing_failed_group** - used when there is data in the WEATHER_RAW staging table signaling that the previous ETL flow failed.
    - Flow tasks:
           run_dbt_models_task          - Transforms raw data using dbt.
        >> empty_raw_table_task         - Empties the WEATHER_RAW table after processing.
        >> load_data_to_staging_task    - Loads fetched data into the WEATHER_RAW table.
        >> run_dbt_models_again_task    - Transforms raw data using dbt.
        >> empty_raw_table_again_task   - Empties the WEATHER_RAW table after processing.

2. **previous_processing_succeeded_group** - used when there is no data in the WEATHER_RAW staging table signaling that the previous ETL flow finished successfully.
    - Flow tasks:
           load_data_to_staging_task    - Loads fetched data into the WEATHER_RAW table.
        >> run_dbt_models_task          - Transforms raw data using dbt.
        >> empty_raw_table_task         - Empties the WEATHER_RAW table after processing.

**check_task >> previous_processing_failed_group**
**check_task >> previous_processing_succeeded_group**





# Python Scripts
1. fetch_data.py - Contains functions to fetch weather data from an external API and load into Snowflake. 
2. configuration.py - Handles the reading of configuration parameters from the database, which include settings like custom and default time intervals.
3. utils.py - Provides utility functions for initializing database connections and other helper methods.
4. hidden_in_plain_sight.py - Defines constants and sensitive information such as API credentials and Snowflake schema names.





# Setup Instructions

## Prerequisites
Python: Ensure Python 3.8+, though 3.12 is advised as it was used to create the entire project.


1. **Clone the repository:**
   ```bash
   git clone https://github.com/razvangvlad/whether_the_weather_will_be_nice_or_not.git
   cd whether_the_weather_will_be_nice_or_not
   ```

2. **Create Python virtual environment**
   ```bash
   python3 -m venv weatherenv
   source weatherenv/bin/activate
   ```

3. **Install Python dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Create Snowflake Database objects**
Connect to the snowflake database using the link provided on request.
Run the Schema.sql SQL statements to initialize the database/schema/source table.


5. **Initialize the DBT project**
It may be required to create the .dbt/ directory in a path of your choice before running the following command:
   ```bash
    dbt init dbt_project
   ```

Povide the following info to the CLI prompts in order to create the profiles.yml in the .git/ directory or create one yourself with the following structure:

dbt_project:
  outputs:
    dev:
      account: [provided upon request]
      database: WEATHER_DATA
      password: [provided upon request]
      role: ACCOUNTADMIN
      schema: POC
      threads: 1
      type: snowflake
      user: [provided upon request]
      warehouse: COMPUTE_WH
  target: dev

Reposition to the dbt_project directory and run dbt debug to check that everything is configured correctly:
   ```bash
   cd dbt_project
   dbt debug
   dbt deps
   ```

Initialize the seed:
   ```bash
   dbt seed
   ```

Note: Keep this terminal open for fast deployment of seed files and open another terminal for the Airflow configuration.


6. **Configure the /airflow/dags/hidden_in_plain_sight.py file**
In order to have access to the API and Snowflake Database, the hidden_in_plain_sight.py file should contain the following information:

API_USER = [provided upon request]
API_PASS = [provided upon request]

SNOWFLAKE_USER = [provided upon request]
SNOWFLAKE_PASSWORD = [provided upon request]
SNOWFLAKE_ACCOUNT = [provided upon request]
SNOWFLAKE_DATABASE = 'WEATHER_DATA'
SNOWFLAKE_SCHEMA = 'POC'
SNOWFLAKE_WAREHOUSE = 'COMPUTE_WH'
SNOWFLAKE_ROLE = 'ACCOUNTADMIN'


7. **Airflow configuration**
Having opened a new terminal in the whether_the_weather_will_be_nice_or_not directory, move to the airflow directory then initialize the airflow services. 
!!! It should be done in the airflow directory in order to have access to the weather_ETL_DAG and for the dynamic paths to work inside the tasks !!!
   ```bash
   export AIRFLOW_HOME=[your path to the local repository]/whether_the_weather_will_be_nice_or_not/airflow
   echo $AIRFLOW_HOME
   cd airflow
   airflow standalone
   ```
Using the username admin and the password provided in the terminal, connect to http://localhost:8080/ and locate the weather_ETL_DAG DAG.


8. **Trigger the DAG**
In order to initialize the entire model and load the last 24 hours of weather data, trigger the DAG with the default configurations.


9. **Further configurations and test scenarios**
The seed files from the DBT project contain 2 implemented configuration tables to be used for the management of the ETL flow.
   I. **Try the DAG Task Group for a Failed Processing**
   - After the initial load is successful, change the ACTIVE column to Yes for the start_datetime_UTC and end_datetime_UTC parameters in seeds/configurations/configuration_parameters.csv to try processing a preset time interval.
   - Run the dbt seed command from the corresponding terminal.
   - Trigger the DAG.
   - Stop the DAG while running the DBT model, before it reaches the cleanup stage.
   - Trigger the DAG again to access the other conditional flow of a failed previous processing.
   - Run dbt test to check all constraints defined.
   - Check that the F_WEATHER_DATA.LOAD_TIMESTAMP column has a different value for the interval specified.

   II. **Check That the Pipeline Does Not Load Any Data from the Future**
   - Change the ACTIVE column to Yes for the start_datetime_UTC and end_datetime_UTC parameters in seeds/configurations/configuration_parameters.csv and set their values to 2024-06-13 22:00:00 to try processing a future time interval.
   - Run the dbt seed command from the corresponding terminal.
   - Trigger the DAG.
   - Check that no data was inserted into the WEATHER_RAW staging table and that both the Airflow and Python module logs state the error clearly.





# Conclusion
This project provides a robust and efficient ETL process for weather data using Airflow and dbt. By following the setup and running instructions, you can ensure that your weather data is processed and transformed correctly for analysis and reporting. For further details on using Airflow and dbt, refer to their official documentation.
