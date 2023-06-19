"""
This file contains methods for establishing the connection to GCP bigquery
"""

from modules.utils.logger import logger
from modules.utils.sparkconnection import sparkconnection


class synapse(sparkconnection, logger):
    def __init__(self, url, driver, user, password, table):
        self.db_url = url
        self.db_driver = driver
        self.db_user = user
        self.db_password = password
        self.table = table
        self.test = sparkconnection

"""
Summary  : Creates a connection to GCP bigquery
Variable : ddl Query
Input    : list[string]
Output   : <yet to decide>
"""


def create_bigquery_connection(connection_parameters: list[str]):
    pass

"""
Summary  : Creates a table in GCP bigquery
Variable : ddl Query
Input    : string
Output   : <yet to decide>
"""


def create_bigquery_table(ddl_query: str):
    pass


"""
Summary  : deletes a table in GCP bigquery
Variable : ddl Query
Input    : string
Output   : <yet to decide>
"""


def delete_bigquery_table(ddl_query: str):
    pass


"""
Summary  : Creates a view in GCP bigquery
Variable : ddl Query
Input    : string
Output   : <yet to decide>
"""


def create_bigquery_view(ddl_query: str):
    pass


"""
Summary  : deletes a view in GCP bigquery
Variable : ddl Query
Input    : string
Output   : <yet to decide>
"""


def delete_bigquery_view(ddl_query: str):
    pass


"""
Summary  : Reads the data from GCP bigquery
Variable : Sql Query
Input    : string
Output   : <yet to decide>
"""


def read_data_from_bigquery(sqlquery: str):
    pass


"""
Summary  : loads the data to the GCP bigquery table
Variable : Sql Query
Input    : string
Output   : <yet to decide>
"""


def load_data_to_bigquery(sqlquery: str):
    pass
