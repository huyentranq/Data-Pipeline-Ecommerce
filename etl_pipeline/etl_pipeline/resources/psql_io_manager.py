from contextlib import contextmanager
import pandas as pd
from dagster import IOManager, OutputContext, InputContext
from sqlalchemy import create_engine

@contextmanager
def connect_psql(config):
    conn_info = (
        f"postgresql+psycopg2://{config['user']}:{config['password']}"
        + f"@{config['host']}:{config['port']}"
        + f"/{config['database']}"
    )
    db_conn = create_engine(conn_info)
    try:
        yield db_conn
    finally:
        db_conn.dispose()

class PostgreSQLIOManager(IOManager):
    def __init__(self, config):
        self._config = config

    def load_input(self, context: InputContext) -> pd.DataFrame:
        schema, table = context.asset_key.path[-2], context.asset_key.path[-1]
        query = f"SELECT * FROM {schema}.{table}"
        with connect_psql(self._config) as conn:
            return pd.read_sql(query, conn)

    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
        schema, table = context.asset_key.path[-2], context.asset_key.path[-1]
        with connect_psql(self._config) as conn:
            obj.to_sql(name=table, con=conn, schema=schema, if_exists='replace', index=False)
