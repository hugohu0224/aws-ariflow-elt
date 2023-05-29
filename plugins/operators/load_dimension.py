from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 conn_id = "",
                 talbe = "",
                 sql_name = "",
                 load_type = "delete-load",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.talbe = talbe
        self.sql_name = sql_name
        self.load_type = load_type



    def execute(self, context):
        # get conn
        redshift_conn = PostgresHook(self.conn_id).get_conn()
        cursor = redshift_conn.cursor()
        # check load_type
        pre_sql = "TRUNCATE TABLE {};".format(self.talbe)
        if self.load_type == "delete-load":
            pass
        elif self.load_type == "append-only":
            pre_sql = "SELECT 1;"
        else:
            raise ValueError(f"load_type Error. load_type must be delete-load or append-only.")
        # get sql
        sql_str = getattr(SqlQueries, self.sql_name)
        # execute sql
        cursor.execute(pre_sql)
        cursor.execute(sql_str)
        redshift_conn.commit()
        self.log.info(f'LoadDimensionOperator({self.sql_name}) finished')