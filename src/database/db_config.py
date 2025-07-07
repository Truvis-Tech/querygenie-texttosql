import configparser
import urllib.parse
from google.cloud.sql.connector import Connector
import os
import sqlalchemy
from src.utils.config_reader import load_config

class GoogleCloudSqlUtility:
    def __init__(self, market):
        self.config = load_config(market)
        self.section = 'GCLOUD_DB'
        self.project_id = self.config.get(self.section, 'project_id')
        self.region = self.config.get(self.section, 'region')
        self.instance_name = self.config.get(self.section, 'instance_name')
        self.database = self.config.get(self.section, 'database')
        self.iam_user = self.config.get(self.section, 'iam_user')
        self.schema = self.config.get(self.section, 'schema')

        self.ip_type = "private"  # Use public since private is enabled
        self.instance_connection_name = f"{self.project_id}:{self.region}:{self.instance_name}"

        self.service_account_file = self.config.get(self.section, 'service_account_file', fallback=None)
        self.dataset_id = self.config.get(self.section, 'dataset_id', fallback=None)
        # Set the environment variable for Google credentials if service_account_file is provided
        if self.service_account_file:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self.service_account_file

    def get_db_connection(self):
        try:
            connector = Connector()
            conn = connector.connect(
                self.instance_connection_name,
                "pg8000",
                user=self.iam_user,
                db=self.database,
                enable_iam_auth=True,
                ip_type=self.ip_type,
            )
            return conn, connector
        except Exception as e:
            print(f"Error: Unable to connect to the database. {e}")
        return None, None

    def execute_query(self, query, params=None, fetch=False):
        conn, connector = self.get_db_connection()
        if not conn:
            return None
        try:
            cursor = conn.cursor()
            cursor.execute(query, params or ())
            if fetch:
                result = cursor.fetchall()
            else:
                result = None
            conn.commit()
            cursor.close()
            return result
        except Exception as e:
            print(f"Error executing query: {e}")
            return None
        finally:
            conn.close()
            connector.close()

    def insert(self, query, params=None):
        return self.execute_query(query, params)

    def select(self, query, params=None):
        return self.execute_query(query, params, fetch=True)

    def update(self, query, params=None):
        return self.execute_query(query, params)

    def delete(self, query, params=None):
        return self.execute_query(query, params)

# Read and execute SQL file
def execute_sql(self,sql_query):
    try:
        self.cursor.execute(sql_query)
    except Exception as e:
        print(f"Error executing Query {sql_query}") 