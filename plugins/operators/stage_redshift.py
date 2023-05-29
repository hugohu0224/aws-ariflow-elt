from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.models import Variable

class StageToRedshiftOperator(BaseOperator):
    
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 conn_id = "",
                 table = "",
                 source_id = "",
                 iam_role_id = "",
                 json_path_id = "",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.table = table
        self.source_id = source_id
        self.iam_role_id = iam_role_id
        self.json_path_id = json_path_id

    def execute(self, context):
        # json sype
        json_str = ""
        if self.json_path_id != "":
            json_str = "JSON '{}'".format(Variable.get(self.json_path_id))
        else :
            json_str = "FORMAT AS JSON 'auto'"
        # inject sql parameter
        sql_str = """
        COPY  {}
        FROM '{}'
        credentials 'aws_iam_role={}'
        {}
        region 'us-east-1'
        """.format(self.table, 
                   Variable.get(self.source_id), 
                   Variable.get(self.iam_role_id),
                   json_str)
        # get conn
        redshift_conn = PostgresHook(self.conn_id).get_conn()
        cursor = redshift_conn.cursor()
        # execute sql
        cursor.execute(sql_str)
        redshift_conn.commit()
        self.log.info('StageToRedshiftOperator finished')

