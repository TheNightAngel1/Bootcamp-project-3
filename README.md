# Medicare Prescription Data ELT
# test test test test

<img src="docs/dec-logo.png" width="250" height="250">

## Project Context 

This project puts you in the shoes of a Solutions Engineering Consultant who has been tapped into to help a company 

### Goals
1. Business solutions relating to Medicare prescription claims volume and potential revenue by state
2. This data pipeline would be valuable to businesses looking to expand medicare approaved pharmacy services or other medicare covered services. This data can be used to locate higer volumes of medicare needs by population across the US

This ELT pipeline uses airbyte, dbt, snowflake and AWS. The architecture diagram below outlines how the ELT pipeline works:
1. A source SQL file is loaded into PostgreSQL
2. Airbyte will extract the data from PostgreSQL and load it into Snowflake
3. DBT will take the raw data from Snowflake and transform it 
4. Using Power BI, we will then take that transformed data and create a semantic layer & visualizations
5. Docker is used to build an image of the solution and AWS will deploy the solution


![airbyte](https://img.shields.io/badge/airbyte-integrate-blue)
![dbt](https://img.shields.io/badge/dbt-transform-blue)
![snowflake](https://img.shields.io/badge/snowflake-database-blue)
![dagster](https://img.shields.io/badge/dagster-orchestrate-blue)

## Introduction 

This project is designed to pull medicare prescription data across all US states. 

![docs/elt-architecture.png](docs/elt-architecture.png)

- [airbyte](https://docs.airbyte.com/)
- [dbt](https://docs.getdbt.com/docs/introduction)
- [dagster](https://docs.dagster.io/getting-started)
- [snowflake](https://docs.snowflake.com/en/)

Accompanying presentation [here](https://bit.ly/dataengineercamp-modern-elt-demo)

## Getting started 

1. Create a new snowflake account [here](https://signup.snowflake.com/)

2. Run the SQL code [snowflake_airbyte_setup.sql](integration/destination/snowflake_airbyte_setup.sql) to configure an Airbyte account in Snowflake 

3. Export the following environment variables 

    ```
    export AIRBYTE_PASSWORD=your_snowflake_password_for_airbyte_account
    export SNOWFLAKE_ACCOUNT_ID=your_snowflake_password
    ```

4. Install the python dependencies

    ```
    pip install -r requirements.txt
    ```

5. Create the mock source database by: 
    - Install [postgresql](https://www.postgresql.org/)
    - Create a new database in your localhost called `dvdrental` 
    - Unzip [dvdrental.zip](integration/source/dvdrental.zip)
    - Use PgAdmin4 to [restore dvdrental](https://www.pgadmin.org/docs/pgadmin4/development/restore_dialog.html)

## Using airbyte 

1. Create a source for the postgresql database `medicare`
    - host: `host.docker.internal`
2. Create a destination for the Snowflake database 
3. Create a connection between `medicare` and `snowflake` 
    - Namespace Custom Format: `<your_destination_schema>`
4. Run the sync job 

## Using snowflake 

1. Log in to your snowflake account 
2. Go to `worksheets` > `+ worksheet` 
3. Query one of the synced tables from airbyte in the raw schema e.g. `select * from medicare.raw.med2021` 
4. On the top left of the worksheet, select `AIRBYTE_DATABASE.AIRBYTE_SCHEMA` 
5. Query one of the synced tables from airbyte e.g. `select * from med2021` 

## Using dbt 

1. `cd` to `transform/dw` 
2. Execute the command `dbt docs generate` and `dbt docs serve` to create the dbt docs and view the lineage graph 
3. Execute the command `dbt build` to run and test dbt models 

## Using dagster 
1. `cd` to `orchestrate/elt` 
2. Execute the command `dagit` to launch the dagit user interface 
3. Go to `workspace` > `jobs` > `elt_job` > `launchpad` > `launch run` to launch the run
