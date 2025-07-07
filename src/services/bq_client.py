"""
BigQuery client setup service.
"""
from google.cloud import bigquery
from google.oauth2 import service_account
from src.database.db_config import GoogleCloudSqlUtility


from src.utils.config_reader import load_config


def get_bigquery_client(market=None):
    """
    Create and return a BigQuery client using the configuration in config.ini.

    Returns:
        tuple: (bigquery.Client, project_id, dataset_id, location)
    """
    db_util = GoogleCloudSqlUtility(market)
    project_id = db_util.project_id
    dataset_id = db_util.dataset_id
    service_account_file = db_util.service_account_file
    location = db_util.bq_location

    # Create credentials from service account file
    credentials = service_account.Credentials.from_service_account_file(service_account_file)

    # Create BigQuery client
    client = bigquery.Client(credentials=credentials, project=project_id, location=location)

    return client, project_id, dataset_id, location