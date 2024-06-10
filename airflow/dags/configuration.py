import utils
import pandas as pd
import hidden_in_plain_sight
from warnings import filterwarnings

# filterwarnings('ignore', message='pandas only supports SQLAlchemy connectable')

def read_parameters_from_database():
    configuration_parameters = dict()
    locations_parameters = dict()

    schema = hidden_in_plain_sight.SNOWFLAKE_SCHEMA + '_' + 'CONFIG'
    conn = utils.get_snowflake_connection(schema=schema)
    try:
        configuration_parameters_df = pd.read_sql("SELECT * FROM CONFIGURATION_PARAMETERS WHERE ACTIVE = 'Yes';", conn)
        print('Successfully read configuration_parameters!')
        locations_parameters_df = pd.read_sql("SELECT * FROM LOCATION_PARAMETERS WHERE ACTIVE = 'Yes';", conn)
        print('Successfully read locations_parameters!')
        data_parameters_df = pd.read_sql("SELECT * FROM DATA_PARAMETERS WHERE ACTIVE = 'Yes' ORDER BY ID;", conn)
        print('Successfully read data_parameters!')
    except Exception as e:
        print(e)
    else:
        list_of_parameters = configuration_parameters_df.to_dict('records')
        configuration_parameters = {item['PARAMETER_NAME']: item['PARAMETER_VALUE'] for item in list_of_parameters}

        list_of_parameters = locations_parameters_df.to_dict('records')
        locations_parameters = {
            item['ID']: {
                'city': item['CITY'],
                'country': item['COUNTRY'],
                'latitude': item['LATITUDE'],
                'longitude': item['LONGITUDE']
            } for item in list_of_parameters
        }
        parameters_ts = [
                        f"{row['DATAITEM_NAME']}_{row['DATAITEM_FREQUENCY']}:{row['DATAITEM_UNIT']}".replace('None_', '').replace('_None:', ':')
                        for _, row in data_parameters_df.iterrows()
                    ]

    finally:
        conn.close()

    return configuration_parameters, locations_parameters, parameters_ts


