import sys

from modules.utils.logger import logger
from modules.utils.sparkconnection import sparkconnection


class Redshift(sparkconnection, logger):
    def __init__(self, url, driver, user, password, table):
        self.db_url = url
        self.db_driver = driver
        self.db_user = user
        self.db_password = password
        self.table = table
        self.test = sparkconnection


"""
Summary  : Creates a connection to AWS redshift
Variable : ddl Query
Input    : list[string]
Output   : <yet to decide>
"""


def create_redshift_connection(self):
    try:
        redshiftconnection = sparkconnection.spark \
            .read.format("jdbc") \
            .option('url', self.db_url) \
            .option('driver', self.db_driver) \
            .option('user', self.db_user) \
            .option('password', self.db_password) \
            .option('dbtable', self.table) \
            .load()
        return redshiftconnection
    except:
        sys.exit(1)
    # add some logger above

"""
Summary  : Creates a table in AWS redshift
Variable : ddl Query
Input    : string
Output   : <yet to decide>
"""


def create_redshift_table(ddl_query: str):
    pass


"""
Summary  : deletes a table in AWS redshift
Variable : ddl Query
Input    : string
Output   : <yet to decide>
"""


def delete_redshift_table(ddl_query: str):
    pass


"""
Summary  : Creates a view in AWS redshift
Variable : ddl Query
Input    : string
Output   : <yet to decide>
"""


def create_redshift_view(ddl_query: str):
    pass


"""
Summary  : deletes a view in AWS redshift
Variable : ddl Query
Input    : string
Output   : <yet to decide>
"""


def delete_redshift_view(ddl_query: str):
    pass


"""
Summary  : Reads the data from AWS Redshift
Variable : Sql Query
Input    : string
Output   : <yet to decide>
"""


def read_data_from_redshift(sqlquery: str):
    pass


"""
Summary  : loads the data to the AWS Redshift table
Variable : Sql Query
Input    : string
Output   : <yet to decide>
"""


def load_data_to_redshift(sqlquery: str):
    pass
