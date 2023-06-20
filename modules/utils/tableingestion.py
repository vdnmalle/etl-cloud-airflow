from modules.utils.parameterutils import paramterutils
from modules.utils.sparkconnection import sparkconnection
from modules.aws.redshift import Redshift,RedshiftOperations


class DataIngestion(sparkconnection, paramterutils, RedshiftConnection,RedshiftOperations):
    def __init__(self, source_name, object_name, logging_bucket_name):
        self.connection_properties = {

            'config_bucket': logging_bucket_name,
            'config_file': 'configs/config.ini',
            'tg_table': 'trgt_' + object_name,  # this target can be changed as per schema
            'stg_table': 'stg_' + object_name,
            'target_schema': 'trgt',
            'stage_schema': 'staging',
            'metadata_schema': 'ctrl',  # change as per the tables created
            'metadata_table': 'ds_metadata',
            'pipeline_table': 'ds_pipeline_list',
            'source': source_name
        }

        self.app_name = self.connection_properties['target_table']
        self.stage_table = self.connection_properties['stage_schema'] + '.' + self.connection_properties['stg_table']
        self.target_table = self.connection_properties['target_schema'] + '.' + self.connection_properties['tg_table']
        self.metadata_table = self.connection_properties['metadata_schema'] + '.' + self.connection_properties[
            'metadata_table']
        self.pipleline_table = self.connection_properties['metadata_table'] + '.' + self.connection_properties[
            'pipeline_table']

        # below are fields about datawarehouse connection , think and decide about how to handle these
        self._config = None
        self.dw_host = None
        self.dw_port = None
        self.dw_user = None
        self.dw_password = None
        self.dw_url = None
        self.dw_dbname = None
        self.db_driver = None

        # below config is for the data storage layer , think and decide on how to handle these commonly across
        self.standard_bucket = None
        self.raw_bucket = None

        ###Initializing spark session
        self.spark = sparkconnection.spark

        ## setting multiple properties for the spark connection , debug and think what can be kept and what all needs to be added

        self.spark.conf.set("spark.sql.crossJoin.enabled", "true")

        paramterutils.set_parameters(self)
        self.dw_url = 'jdbc:<tbd>://' \
                      + self.host \
                      + ':' \
                      + self.port \
                      + '/' \
                      + self.dbname \
                      + '?' \
                      + 'user=' \
                      + self.user \
                      + '&' \
                      + 'password=' \
                      + self.password
        self.temp = '<tbd>://' + self.temp_bucket + '/' + self.temp

        RedshiftConnection.create_redshift_connection(self)
        RedshiftOperations.load_data_to_redshift(sqlquery="<TBD>")

