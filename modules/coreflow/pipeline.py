from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator

from modules.rawdataextraction.rawdataeatraction import rawdataextraction
from modules.rawdataextraction.sourcedata import sourcedata
from modules.utils.loadstatusupdate import loadstatusupdate


class pipeline(loadstatusupdate):
    @staticmethod
    def start_file_processing():
        task = EmptyOperator(task_id="started file processing")
        return task

    def data_extract_from_source(self, object_name):
        task = BranchPythonOperator(
            task_id="Extracting_Raw_Data_{}".format(object_name),
            python_callable=sourcedata,
            op_kwargs={"sourcetype": "gcs", "credentials": "credentials"}
        )
        return task

    def raw_data_load_started(self, object_name):
        task = BranchPythonOperator(task_id=
                                    "Raw_{}_Data_load_to_Staging_layer".format(object_name))
        return task

    def raw_to_standard_data(self, object_name):
        task = BranchPythonOperator(task_id="staging_to_standard_{}".format(object_name))
        return task

    def warehouse_load_starts(self, object_name):
        task = EmptyOperator(task_id="warehouse_load_started_{}".format(object_name))
        return task

    def warehouseloading(self, object_name):
        task = BranchPythonOperator(task_id="warehouse_data_load_{}".format(object_name))
        return task

    def checkwarehousedataloadedstatus(self, object_name):
        task = BranchPythonOperator(task_id="check_warehouse_loaded_status_{}".format(object_name))
        return task

    def completedload(self, object_name):
        task = BranchPythonOperator(task_id="data_load_completed_for_{}".format(object_name))
        return task

    def faileddataload(self, object_name):
        task = BranchPythonOperator(task_id="failed_data_load_for_{}".format(object_name))
        return task

    def workflowcompleted(self, object_name):
        task = EmptyOperator(task_id="workflow_completed_for_{}".format(object_name))
        return task
