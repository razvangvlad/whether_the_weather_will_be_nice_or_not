import hidden_in_plain_sight
import snowflake.connector
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL

def initialize_engine(
            user = None,
            password = None,
            account = None,
            database = None,
            schema = None,
            warehouse = None,
            role = None):
    
    if user == None:
        user=hidden_in_plain_sight.SNOWFLAKE_USER
    if password == None:
        password=hidden_in_plain_sight.SNOWFLAKE_PASSWORD
    if account == None:
        account=hidden_in_plain_sight.SNOWFLAKE_ACCOUNT
    if database == None:
        database=hidden_in_plain_sight.SNOWFLAKE_DATABASE
    if schema == None:
        schema=hidden_in_plain_sight.SNOWFLAKE_SCHEMA
    if warehouse == None:
        warehouse=hidden_in_plain_sight.SNOWFLAKE_WAREHOUSE
    if role == None:
        role=hidden_in_plain_sight.SNOWFLAKE_ROLE

    engine = create_engine(URL(
        user=user,
        password=password,
        account=account,
        database=database,
        schema=schema,
        warehouse=warehouse,
        role=role
    ))

    return engine

def get_snowflake_connection(
            user = None,
            password = None,
            account = None,
            database = None,
            schema = None,
            warehouse = None,
            role = None):

    if user == None:
        user=hidden_in_plain_sight.SNOWFLAKE_USER
    if password == None:
        password=hidden_in_plain_sight.SNOWFLAKE_PASSWORD
    if account == None:
        account=hidden_in_plain_sight.SNOWFLAKE_ACCOUNT
    if database == None:
        database=hidden_in_plain_sight.SNOWFLAKE_DATABASE
    if schema == None:
        schema=hidden_in_plain_sight.SNOWFLAKE_SCHEMA
    if warehouse == None:
        warehouse=hidden_in_plain_sight.SNOWFLAKE_WAREHOUSE
    if role == None:
        role=hidden_in_plain_sight.SNOWFLAKE_ROLE

    conn = snowflake.connector.connect(
        user=user,
        password=password,
        account=account,
        database=database,
        schema=schema,
        warehouse=warehouse,
        role=role
    )
    return conn
