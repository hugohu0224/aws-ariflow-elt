from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.models import Variable
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 conn_id = "",
                 dq_checks = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.dq_checks = dq_checks

    def execute(self, context, *args, **kwargs):
        # get conn
        redshift_conn = PostgresHook(self.conn_id).get_conn()
        cursor = redshift_conn.cursor()
        # loop dq_checks to execute sqls
        for dq_check in self.dq_checks:
            # get parameter
            check_sql = dq_check['check_sql']
            expected_result = dq_check['expected_result']
            # execute sql
            cursor.execute(check_sql)
            redshift_conn.commit()
            result = cursor.fetchall()
            # checking data
            if result[0][0] > 0:
                raise ValueError(f"Data quality check failed. SQL:{check_sql}, RESULT:{result}, EXPECTED:{expected_result}.")
            if len(result) < 1 or len(result[0]) < 1:
                raise ValueError(f"Data quality check failed. SQL:{check_sql}, returned no results")
            self.log.info(f'DataQualityOperator SQL:{check_sql} finished')
        self.log.info(f'DataQualityOperator finished')