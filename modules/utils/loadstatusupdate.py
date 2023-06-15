import sys


class loadstatusupdate():
    def __init__(self, ip_load_status):
        self.loadstatus = ip_load_status


"""
Description : This method is used to update the loadstatus during the pipeline
Variables   : str
input       : load_status variable
output      : None , SQL table will be updated


"""

    def load_type_update(self):

        try:
            if self.loadstatus == 'STARTS':
                sql_query = '''
                update {table_name} set load_status = {load_status}, etl_updated_date = sysdate where source_system = '{source_system}'
                and active_flag = '{flag}'
                '''.format(table_name='load_status_table', load_status=self.loadstatus, source_system='<TBD>',
                           flag='<TBD')
                pass
            elif self.loadstatus == 'COMPLETED':
                last_load_timestamp = '''
                select max(load_timestamp) as max_load_timestamp from {table} where source_system = '{source_system}'
                and <conditions TBD>
                '''.format(table='<TBD>', source_system='<TBD')
                ### add more elif blocks if you need to handle more conditions
                pass

            else:
                # add logger method here
                pass

        except Exception as e:
            sys.exit(1)
