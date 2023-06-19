"""
This file contains methods for establishing the connection to Azure Synapse
"""
from modules.utils.sparkconnection import sparkconnection
from modules.utils.logger import logger


class synapse(sparkconnection, logger):
    def __init__(self, url, driver, user, password, table):
        self.db_url = url
        self.db_driver = driver
        self.db_user = user
        self.db_password = password
        self.table = table
        self.test = sparkconnection


"""
Summary  : Creates a connection to Azure synapse
Variable : ddl Query
Input    : list[string]
Output   : <yet to decide>
"""


def create_synapse_connection(connection_parameters: list[str]):
    pass


"""
Summary  : Creates a table in Azure synapse
Variable : ddl Query
Input    : string
Output   : <yet to decide>
"""


def create_synapse_table(ddl_query: str):
    pass


"""
Summary  : deletes a table in Azure synapse
Variable : ddl Query
Input    : string
Output   : <yet to decide>
"""


def delete_synapse_table(ddl_query: str):
    pass


"""
Summary  : Creates a view in Azure synapse
Variable : ddl Query
Input    : string
Output   : <yet to decide>
"""


def create_synapse_view(ddl_query: str):
    pass


"""
Summary  : deletes a view in Azure synapse
Variable : ddl Query
Input    : string
Output   : <yet to decide>
"""


def delete_synapse_view(ddl_query: str):
    pass


"""
Summary  : Reads the data from Azure synapse
Variable : Sql Query
Input    : string
Output   : <yet to decide>
"""


def read_data_from_synapse(sqlquery: str):
    pass


"""
Summary  : loads the data to the Azure synapse table
Variable : Sql Query
Input    : string
Output   : <yet to decide>
"""


def load_data_to_synapse(sqlquery: str):
    pass
