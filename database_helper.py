import pandas as pd
import json

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session


# load config file
with open('config.json', 'r') as f:
    config = json.load(f)

# load pg credentials
with open('dbCredentials.json', 'r') as f:
    pg_credentials = json.load(f)


class Postgres:
    
    def __init__(self) -> None:
        self.engine = create_engine(
            f"postgresql://{pg_credentials['username']}:{pg_credentials['password']}@{config['database']['host']}:{config['database']['port']}/{config['database']['dbName']}",
            connect_args={"connect_timeout": 15}
        )

    def exec_query(self, query_txt: str) -> None:
        with Session(self.engine) as session, session.begin():
            session.execute(
                text(query_txt).execution_options(autocommit=True)
            )

    def exec_insert(self, df: pd.DataFrame, schema: str, table: str) -> None:
        df.to_sql(
            con=self.engine,
            schema=schema,
            name=table,
            if_exists='append',
            index=False
        )

    def get_data_from_sql(self, query_txt: str) -> pd.DataFrame:
        df = pd.read_sql(
            sql=text(query_txt),
            con=self.engine.connect()
        )

        return df

    def execute_stored_procedure(self, call_txt: str) -> None:
        with Session(self.engine) as session, session.begin():
            session.execute(
                text(
                    call_txt
                ).execution_options(autocommit=True)
            )

    def exec_truncate_table(self, schema: str, table: str) -> None:
        with Session(self.engine) as session, session.begin():
            session.execute(
                text(f'truncate table {schema}.{table}').execution_options(autocommit=True)
            )