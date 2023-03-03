from database_helper import Postgres


def create_objects(pg: Postgres, file_path: str) -> None:
    with open(file_path, 'r') as f:
        file = f.read().split('\n\n')
    
    for query in file:
        try:
            print(f'Executing query: {query[:50]}')
            pg.exec_query(query)
        except Exception as e:
            print(f'Query: {query[:50]} failed')
            print(e)
        finally:
            print('\n')

def main():
    # init pg database object
    pg_obj = Postgres()

    # create schemas
    SCHEMA_FILE_PATH = 'queries/create_objects/00-create_schemas.sql'
    create_objects(
        pg=pg_obj,
        file_path=SCHEMA_FILE_PATH
    )

    # create tables
    TABLE_FILE_PATH = 'queries/create_objects/01-create_tables.sql'
    create_objects(
        pg=pg_obj,
        file_path=TABLE_FILE_PATH
    )

    # create procedures
    SP_FILE_PATH = 'queries/create_objects/02-create_stored_procedures.sql'
    create_objects(
        pg=pg_obj,
        file_path=SP_FILE_PATH
    )

if __name__ == '__main__':
    main()