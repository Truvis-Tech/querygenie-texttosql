from src.utils.config_reader import load_config

def get_postgres_connection_params(marketname):
    config = load_config(marketname)

    host = config['POSTGRES']['host']
    port = config['POSTGRES']['port']
    database = config['POSTGRES']['database']
    user = config['POSTGRES']['user']
    password = config['POSTGRES']['password']

    return host, port, database, user, password
