from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 conn_id = "",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id

    def execute(self, context):
        # get conn
        redshift_conn = PostgresHook(self.conn_id).get_conn()
        cursor = redshift_conn.cursor()
        # execute sql
        cursor.execute(SqlQueries.songplay_table_insert)
        redshift_conn.commit()
        self.log.info('LoadFactOperator finished')
