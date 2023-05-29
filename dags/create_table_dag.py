from datetime import datetime, timedelta
import os
import sys
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.secrets.metastore import MetastoreBackend 

# from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
#                                 LoadDimensionOperator, DataQualityOperator)

# get the directory of the current script
script_dir = os.path.dirname(os.path.abspath(__file__))
# add the parent directory to the sys.path
sys.path.insert(0, os.path.dirname(script_dir))
from plugins.helpers.create_tables_py import ct_artists, ct_songplays, ct_songs, ct_staging_events, ct_staging_songs, ct_time, ct_users

default_args = {
    'owner': 'Hugo', 
    'start_date': datetime(2023, 5, 19),
    'depends_on_past': False,  # The DAG does not have dependencies on past runs
    'retries': 3,  # On failure, the task are retried 3 times
    'catchup': False,  # Catchup is turned off
}

with DAG('create_table_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        ) as dag:

    start_operator = DummyOperator(
        task_id='Begin_execution'
        )

    crearte_statements = [ct_artists, ct_songplays, ct_songs, ct_staging_events, ct_staging_songs, ct_time, ct_users]
    for i, sql in enumerate(crearte_statements):
        create_table_task = PostgresOperator (
        task_id = f'{i}',
        postgres_conn_id = "redshift" ,
        sql = sql
        ) 
    