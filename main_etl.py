import json

import numpy as np
import pandas as pd
from database_helper import Postgres


"""
In our case, etl will look something like:
1. extract from flat file
2. prepare data before ingesting it to staging
3. ingest data to staging
4. execute stored procedures used to ingest data from staging to dim and fact tables
5. truncate staging table

The will not continue if any of the steps fails
"""

# load config file
with open('config.json', 'r') as f:
    config = json.load(f)

# global variables 
DATA_FILE_PATH = 'data/Video_Games_Sales_as_at_22_Dec_2016.csv'


def ETL_extract(file_path: str) -> pd.DataFrame:
    print(f'Extracting file {file_path}')
    df = pd.read_csv(
        file_path
    )

    return df

def ETL_transform(df: pd.DataFrame) -> None:
    print(f'Transforming df data')

    # if there are two rows with same "primary key" values, remove them
    df.drop_duplicates(
        subset=config['uniqueColumns'],
        keep=False,
        inplace=True
    )
    
    # convert columns to milions e.g. 2.3 -> 2 300 000
    for column in config['milionColumns']:
        df[column] = df[column] * 1000000

    # replace "dummy null" values with nan
    df.replace(
       to_replace=config['replaceWithNan'],
       value=np.NaN,
       inplace=True
    )

    # rename columns to lowercase to match with pg column names
    mapper = {column: column.lower() for column in df.columns}
    df.rename(
        columns=mapper,
        inplace=True
    )

    print('DataFrame transformed successfully\n')

def ETL_load(pg: Postgres, df: pd.DataFrame, schema: str, table: str) -> None:
    print(f'Executing insert into {schema}.{table}')
    pg.exec_insert(
        df=df,
        schema=schema,
        table=table,
    )
    print('Loading executed successfully\n')

def call_stored_procedures(pg: Postgres) -> None:
    for stmt in config['callProceduresByOrder']:
        print(f'Executing stored procedure {stmt}')
        pg.execute_stored_procedure(
            call_txt=stmt
        )
    print('Procedures executed successfully\n')
    

def truncate_staging(pg: Postgres, schema: str, table: str):
    print(f'Executing truncate of the table {schema}.{table}')
    pg.exec_truncate_table(
        schema=schema,
        table=table
    )
    print('Table truncated successfully\n')
    

def main():
    # create pg object
    pg_obj = Postgres()

    # ETL - E
    df = ETL_extract(
        file_path=DATA_FILE_PATH
    )

    # ETL - T
    ETL_transform(
        df=df
    )

    # ETL - L
    ETL_load(
        pg=pg_obj,
        df=df,
        schema=config['targetStg']['schema'],
        table=config['targetStg']['table'],
    )
    
    # transfer data to dm - execute procedures
    call_stored_procedures(
        pg=pg_obj
    )
    
    # truncate stg table
    truncate_staging(
        pg=pg_obj,
        schema=config['targetStg']['schema'],
        table=config['targetStg']['table'],
    )


if __name__ == '__main__':
    main()
