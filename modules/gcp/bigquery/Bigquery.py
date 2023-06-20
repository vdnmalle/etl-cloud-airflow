"""
This file contains methods for establishing the connection to GCP bigquery
"""
import sys

from modules.utils.logger import logger
from modules.utils.sparkconnection import sparkconnection


class bigquery(sparkconnection, logger):
    def __init__(self, url, driver, user, password):
        self.db_url = url
        self.db_driver = driver
        self.db_user = user
        self.db_password = password
        self.test = sparkconnection

    def create_bigquery_connection(self,tablename):
        try:
            redshiftconnection = sparkconnection.spark \
                .read.format("jdbc") \
                .option('url', self.db_url) \
                .option('driver', self.db_driver) \
                .option('user', self.db_user) \
                .option('password', self.db_password) \
                .option('dbtable', tablename) \
                .load()
            return redshiftconnection
        except:
            sys.exit(1)

    def create_bigquery_table(ddl_query: str):
        pass

    def delete_bigquery_table(ddl_query: str):
        pass

    def create_bigquery_view(ddl_query: str):
        pass

    def delete_bigquery_view(ddl_query: str):
        pass

    def read_data_from_bigquery(sqlquery: str):
        pass
    def load_data_to_bigquery(sqlquery: str):
       pass
