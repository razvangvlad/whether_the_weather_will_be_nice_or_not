import configuration as cfg
import hidden_in_plain_sight
import utils
from datetime import datetime, timedelta, UTC
import meteomatics.api as api
from snowflake.connector.pandas_tools import write_pandas
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker
from logging import basicConfig, getLogger, INFO, Formatter
from logging.handlers import RotatingFileHandler
from os import path, getcwd, makedirs
from sys import exc_info
from traceback import format_exception
from pytz import UTC as pytz_UTC

def initialize_logger():
    basicConfig(level=INFO
                ,format='%(asctime)s - %(levelname)s - %(message)s')

    log_files_folder_path = path.abspath(path.join(getcwd(), 'python_logs'))
    if not path.exists(log_files_folder_path):
        makedirs(log_files_folder_path)

    log_files_path = path.join(log_files_folder_path,'python_module.log')

    rotating_handler = RotatingFileHandler(log_files_path, maxBytes= 3145728, backupCount=5)
    rotating_handler.setLevel(INFO)
    formatter = Formatter('%(asctime)s - %(levelname)s - %(message)s')
    rotating_handler.setFormatter(formatter)

    logger = getLogger('python_module')
    logger.addHandler(rotating_handler)

    return logger

def get_max_date_from_date_dimension(logger=None):
    schema = hidden_in_plain_sight.SNOWFLAKE_SCHEMA + '_' + 'MARTS'

    engine = utils.initialize_engine(schema=schema)
    logger.info(f'Initilized the sqlalchemy engine. Schema = "{schema}".')

    with engine.connect() as connection:
        logger.info(f'Initilized the connection to the snowflake database.')
        try:
            result = connection.execute(text("SELECT MAX(DATE) FROM D_DATE")).fetchone()
        except:
            logger.warning(f'The D_DATE table does not yet exist. It will be created after the first dbt run.')
            return None

        if len(result) > 0:
            logger.info(f'Read the maximum date value from the date dimension.')
            original_timestamp_tz = result[0]
            local_timestamp_tz = original_timestamp_tz.astimezone(pytz_UTC)
            return local_timestamp_tz
    
    logger.info(f'The database is empty. It is the first run and the default_time_interval_hours parameter applies.')
    return None

def add_unit_columns(df):
    new_columns = {}
    df.reset_index(inplace=True)
    initial_df_columns = df.columns
    for col in initial_df_columns:
        if ':' in col:
            base_col, unit = col.split(':')
            new_col_name = f'{base_col}_unit'.upper()
            new_columns[col] = base_col.upper()
            if 'weather_symbol' not in col:
                df[new_col_name] = unit
        else:
            new_columns[col] = col.upper()

    df.rename(columns=new_columns, inplace=True)

    return df

def fetch_weather_data(logger):
    configuration_parameters, locations_parameters, parameters_ts = cfg.read_parameters_from_database()
    logger.info('Successfully read the parameters from the database.')

    present_time = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    if 'start_datetime_UTC' in configuration_parameters and 'end_datetime_UTC' in configuration_parameters:
        startdate_ts = datetime.strptime(configuration_parameters['start_datetime_UTC'], '%Y-%m-%d %H:%M:%S').replace(minute=0, second=0, microsecond=0)
        enddate_ts = datetime.strptime(configuration_parameters['end_datetime_UTC'], '%Y-%m-%d %H:%M:%S').replace(minute=0, second=0, microsecond=0)
        
        startdate_ts = startdate_ts.replace(tzinfo=pytz_UTC)
        enddate_ts = enddate_ts.replace(tzinfo=pytz_UTC)
        
        if startdate_ts > enddate_ts:
            raise RuntimeError('The start_datetime_UTC parameter cannot be higher than the end_datetime_UTC parameter. Configure the values properly using the configuration_parameters seed.')
        if enddate_ts > present_time:
            logger.info('No additional data available. Forecasting future weather is prohibited; the ETL process only retrieves actual historical data.')
            return None

    else:
        last_imported_date = get_max_date_from_date_dimension(logger)  
        enddate_ts = present_time

        if last_imported_date is not None:
            startdate_ts = last_imported_date + timedelta(hours=1)
        else:
            startdate_ts = present_time - timedelta(hours=int(configuration_parameters['default_time_interval_hours']))

    if startdate_ts >= present_time:
        logger.info('No additional data available. Forecasting future weather is prohibited; the ETL process only retrieves actual historical data.')
        return None

    logger.info(f'Fetching API data for {startdate_ts} - {enddate_ts} (UTC)')
    interval_ts = timedelta(hours=1)
    coordinates_ts = [(location['latitude'], location['longitude']) for location in locations_parameters.values()]

    parameters_ts = parameters_ts
    model = 'mix'
    ens_select = None
    cluster_select = None
    username = hidden_in_plain_sight.API_USER
    password = hidden_in_plain_sight.API_PASS

    df_ts = api.query_time_series(coordinates_ts, startdate_ts, enddate_ts, interval_ts, parameters_ts,
                                    username, password, model, ens_select, cluster_select)
    logger.info('Read the data from the API.')
    df_ts = add_unit_columns(df_ts)

    return df_ts

def load_data_to_raw_table():
    logger = initialize_logger()

    try:
        df = fetch_weather_data(logger)
        if df is not None:
            schema = hidden_in_plain_sight.SNOWFLAKE_SCHEMA + '_' + 'STAGING'
            conn = utils.get_snowflake_connection(schema=schema)
            logger.info(f'Initilized the connection to the database. Schema = "{schema}".')

            success, nchunks, nrows, _ = write_pandas(conn, df, 'WEATHER_RAW', use_logical_type=True)
            if success:
                logger.info(f'Data successfully loaded into the staging WEATHER_RAW table with {nrows} rows.')
            else:
                raise Exception('Failed to load data into the staging WEATHER_RAW table.')

    except Exception as e:
        exception_message = str(e)

        exc_type, exc_value, exc_traceback = exc_info()
        traceback_info = format_exception(exc_type, exc_value, exc_traceback)
        
        full_exception_info = f"{exception_message}\n{''.join(traceback_info)}"
        logger.critical(full_exception_info)
        raise RuntimeError(e)

    finally:
        try:
            conn.close()
        except:
            pass

def check_raw_table_empty():
    logger = initialize_logger()
    logger.info(f'Checking if the staging WEATHER_RAW table is empty.')
    schema = hidden_in_plain_sight.SNOWFLAKE_SCHEMA + '_' + 'STAGING'
    try:
        engine = utils.initialize_engine(schema=schema)
        logger.info(f'Initilized the sqlalchemy engine. Schema = "{schema}".')

        with engine.connect() as connection:
            logger.info(f'Initilized the connection to the snowflake database.')
            result = connection.execute(text("SELECT COUNT(*) FROM WEATHER_RAW")).fetchone()
            if result[0] > 0:
                logger.info(f'The staging WEATHER_RAW table is not empty. The previous ETL flow failed.')
                return 1  
        return 0

    except Exception as e:
        exception_message = str(e)

        exc_type, exc_value, exc_traceback = exc_info()
        traceback_info = format_exception(exc_type, exc_value, exc_traceback)
        
        full_exception_info = f"{exception_message}\n{''.join(traceback_info)}"
        logger.critical(full_exception_info)
        raise RuntimeError(e)

def empty_raw_table():
    logger = initialize_logger()

    schema = hidden_in_plain_sight.SNOWFLAKE_SCHEMA + '_' + 'STAGING'
    try:
        engine = utils.initialize_engine(schema=schema)
        logger.info(f'Initilized the sqlalchemy engine. Schema = "{schema}".')

        Session = sessionmaker(bind=engine)
        session = Session()
        logger.info(f'Initilized the sqlalchemy session.')

        session.execute(text("TRUNCATE TABLE WEATHER_RAW;"))
        logger.info(f'Truncated the WEATHER_RAW table.')
        session.commit()
        logger.info(f'The truncate table statement has been committed.')
        
    except Exception as e:
        try:
            session.rollback()
            logger.warning(f'The truncate table statement has been rolled back!')
        except:
            pass

        exception_message = str(e)

        exc_type, exc_value, exc_traceback = exc_info()
        traceback_info = format_exception(exc_type, exc_value, exc_traceback)
        
        full_exception_info = f"{exception_message}\n{''.join(traceback_info)}"
        logger.critical(full_exception_info)
        raise RuntimeError(e)
    
    finally:
        try:
            session.close()
            logger.info(f'Closed the sqlalchemy session.')
        except:
            pass

