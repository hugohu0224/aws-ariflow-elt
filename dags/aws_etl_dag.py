from datetime import datetime, timedelta
import os
import sys
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.secrets.metastore import MetastoreBackend 

# from airflow.operators import StageToRedshiftOperator, DataQualityOperator, LoadFactOperator, LoadDimensionOperator

# get the directory of the current script
script_dir = os.path.dirname(os.path.abspath(__file__))
# add the parent directory to the sys.path
sys.path.insert(0, os.path.dirname(script_dir))
from plugins.operators.stage_redshift import StageToRedshiftOperator
from plugins.operators.load_fact import LoadFactOperator
from plugins.operators.load_dimension import LoadDimensionOperator
from plugins.operators.data_quality import DataQualityOperator
from plugins.helpers.sql_queries import SqlQueries

default_args = {
    'owner': 'Hugo', 
    'start_date': datetime(2023, 5, 22),
    'depends_on_past': False,  # The DAG does not have dependencies on past runs
    'retries': 3,  # On failure, the task are retried 3 times
    'retry_delay': timedelta(minutes=5),  # Retries happen every 5 minutes
    'catchup': False,  # Don't catchup
    'email_on_retry': False,  # Do not email on retry
}

with DAG('aws_etl_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        ) as dag:

    start_operator = DummyOperator(
        task_id='Begin_execution'
        )

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        conn_id='redshift',
        table='staging_events',
        source_id='s3_log_data',
        iam_role_id = 'my-redshift-service-role',
        json_path_id='s3_json_path'
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        conn_id='redshift',
        table='staging_songs',
        source_id='s3_song_data',
        iam_role_id = 'my-redshift-service-role',
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        conn_id='redshift'
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        sql_name= 'user_table_insert',
        conn_id='redshift',
        talbe = 'users'
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        sql_name= 'song_table_insert',
        conn_id='redshift',
        talbe = 'songs'
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        sql_name= 'artist_table_insert',
        conn_id='redshift',
        talbe = 'artists'
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        sql_name= 'time_table_insert',
        conn_id='redshift',
        talbe = 'time'
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        conn_id='redshift',
        dq_checks = [
            {'check_sql':'SELECT COUNT(CASE WHEN userid IS NULL THEN 1 END) FROM songplays', 'expected_result': 0},
            {'check_sql':'SELECT COUNT(CASE WHEN song_id IS NULL THEN 1 END) FROM staging_songs', 'expected_result': 0}
        ]
    )

    end_operator = DummyOperator(
        task_id='Stop_execution',
    )

    # step 1 load data from S3 ro Redshift staging table
    start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]

    # step 2 load data from staging table to fact table
    [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table

    # step 3 load data from staging table to dimension table
    load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] 

    # step 4 check data quality
    [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table,  load_time_dimension_table] >> run_quality_checks

    # step 5 end
    run_quality_checks >> end_operator
