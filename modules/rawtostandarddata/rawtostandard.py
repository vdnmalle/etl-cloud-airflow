import datetime
import logging
from datetime import datetime
import sys
import os
from pyspark import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_utc_timestamp, current_timestamp

from modules.gcp.bigquery.Bigquery import bigquery
from modules.utils.logger import logger


class rawtostandarddata(bigquery, logger):
    def __init__(self, source_system, input_object_name, logger_bucket_name, root_path_bucket):
        self.app_name = source_system + '_rawtostandard'
        self.rootpath_bucket = root_path_bucket
        ### Create context############
        self.spark = SparkSession.builder \
            .appName(self.app_name) \
            .config("spark.num.executors", "24") \
            .config("spark.driver.memory", "88g") \
            .config("spark.executor.memory", "88g") \
            .config("spark.driver.cores", "12") \
            .config("spark.executor.cores", "12") \
            .config("spark.scheduler.mode", "FAIR") \
            .getOrCreate()
        # for all the above find a way to pass all the above configs from the spark-submit dynamically for each job rather than sending like this
        # find a way to optimize the cluster usage by using latest techniques like ADQ etc
        # work on the above things and update the cluster

        self.sc = self.spark.sparkContext
        self.sqlContext = SQLContext(self.sc)

        ###   Intializing the  the Variables ####

        ### reading the data for database connections#
        ### at this point maintained in a file .But, please move these to secured place rather than in a storage bucket##
        # below root path should be like gs for gcp and s3 for aws etc
        self.secured_info = root_path_bucket + '://' + logger_bucket_name + '/configs/env_sec_info.csv'
        df_secured_info = self.spark.read.format('csv').options(header='true', inferSchema='true').load(
            self.secured_info) \
            .load(self.secured_info)

        ###  read the different columns required from the secure info"

        df_sec = not df_secured_info.select('raw_bucket', 'standard_bucket', 'logging_bucket', 'database_url',
                                            'database_driver', 'database_port', 'database_user', 'database_pwd',
                                            'database_github', 'db_name').where(
            df_secured_info.task == '<TBD').collect()[0]

        ### gather the variables required ###

        self.raw_bucket = df_sec[0]
        self.standard_bucket = df_sec[1]
        self.logging_bucket = df_sec[2]
        self.database_url = df_sec[3]
        self.database_driver = df_sec[4]
        self.database_port = df_sec[5]
        self.database_user = df_sec[6]
        self.database_pwd = df_sec[7]
        self.database_github = df_sec[8]
        self.database_name = df_sec[9]
        self.source_system = source_system
        self.object_name = input_object_name
        self.logging_bucket = logger_bucket_name
        self.ds_load_type = 'ctrl.ds_load_type'
        self.ds_audit_log = 'ctrl.ds_audit_log'
        self.ds_pipeline_list = 'ctrl.ds_pipeline_list'
        self.tempdir = root_path_bucket + '//' + self.logging_bucket + '/' + self.source_system + '/' + 'temp'
        self.log_file = source_system + '/Log/' + self.source_system + '_rawtostandard_log.txt'
        ############initialize multiple database names here #############

        ##########Initialize the time/dates of different format to use here
        self.now = datetime.today()
        self.datetime = self.now.strftime('%Y-%m-%d %H:%M:%S')
        self.date = self.now.strftime('%Y-%m-%d')
        self.date_str = self.now.strftime('%Y%m%d')
        self.hour = self.now.strftime('%H')
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.INFO)

    def get_objects_list(self, ds_load_type, ds_audit_log, ds_pipeline_list):
        pass
        # here write the logic to get the objects that need to processed . read more about this
        df_main = self.sqlContext.sql("select 1")
        return df_main

    def raw2curated_main(self):
        err_code = 0
        load_hour_count = 1
        try:
            ds_load_type = bigquery.create_bigquery_connection(self.ds_load_type)
            ds_audit_log = bigquery.create_bigquery_connection(self.ds_audit_log)
            ds_pipeline_list = bigquery.create_bigquery_connection(self.ds_pipeline_list)
            ds_main = self.get_objects_list(ds_load_type, ds_audit_log, ds_pipeline_list)
            prev_obj_name = ''
            for row in ds_main.toLocalIterator():
                source_system = row['source_system']
                object_name = row['object_name']
                standard_bucket = row['standard_bucket']
                raw_bucket = row['raw_bucket']
                incr_standard_bucket = row['incr_standard_bucket']
                ods_standard_s3_bucket = row['ods_standard_bucket']
                load_type = row['load_type']
                load_date = row['load_date']
                load_hour = row['load_hour']
                obj_count = row['obj_count']
                pipeline_name = row['pipeline_name']
                standard_base_bucket = row['standard_base_bucket']
                try:
                    process_start_date = self.spark.range(1).select(from_utc_timestamp
                                                                    (current_timestamp(),
                                                                     'America/Los_Angeles')).collect()[0]
                    start_date_time = process_start_date[0]
                    logger.console_log(self, msg='process started at :{}'.format(start_date_time))
                    ############here implement the logic for the source to standard process"#####
                except Exception as e:
                    logger.console_log(self, msg='>>>>>>>>>error occured in raw to standard process')
                    logger.console_log(self, msg=str(e), error=True)
        except Exception as e:
            logger.console_log(self, msg='>>>>>>>>>>>>>>>>>>>Error in raw 2 standard process')
            logger.console_log(msg=str(e), error=True)
            err_code = 1
            sys.exit(1)
        if err_code != 1:
            logger.console_log(self, msg='>>>>>>>>>>>>>raw to standard process ends')


def main():
    if len(sys.argv) != 4:
        sys.exit("Invalid number of arguments for the program")

        source_system = sys.argv[1]
        object_name = sys.argv[2]
        operations_bucket = sys.argv[3]
        rootpathofBucket = sys.argv[4]
    try:
        op = rawtostandarddata(source_system, object_name, operations_bucket, rootpathofBucket)
        op.raw2curated_main()
    except:
        sys.exit(1)


if __name__ == "__main__":
    main()
