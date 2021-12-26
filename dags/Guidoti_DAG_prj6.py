from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (
    StageToRedshiftOperator, 
    LoadFactOperator,
    LoadDimensionOperator, 
    DataQualityOperator
)
from airflow.operators.subdag_operator import SubDagOperator

from helpers import SqlQueries, CreateTables, DataQuality
from Guidoti_SubDAG_prj6 import create_tables

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'Marcus Guidoti | Udacity Data Engineer Nanodegree Student',
#     'start_date': datetime(2019, 1, 12),
    'start_date': datetime(2021, 12, 25),
    'depends_on_past': False,
    'email_on_failure': False,
    'catchup': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('Guidoti_Prj6_DEND_DAG',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
#           schedule_interval='0 * * * *'
          schedule_interval=None
        )

start_task = SubDagOperator(
    subdag=create_tables(parent_dag_id = "Guidoti_Prj6_DEND_DAG",
                         task_id = "Start_creating_tables_task",
                         redshift_conn_id = "redshift",
                         create_tables = CreateTables.ddls,
                         start_date = datetime.utcnow()),
    task_id = "Start_creating_tables_task",
    dag = dag,
)
        
stage_events_to_redshift = StageToRedshiftOperator(
    task_id = 'Stage_events',
    dag = dag,
    redshift_conn_id = "redshift",
    aws_credentials_id = "aws_credentials",
    table = "staging_events",
    s3_bucket = "udacity-dend",
    s3_key = "log_data",
    json_mode = "'s3://udacity-dend/log_json_path.json'"
)

stage_songs_to_redshift = DummyOperator(task_id='Stage_songs',  dag=dag)
# stage_songs_to_redshift = StageToRedshiftOperator(
#     task_id = 'Stage_songs',
#     dag = dag,
#     redshift_conn_id = "redshift",
#     aws_credentials_id = "aws_credentials",
#     table = "staging_songs",
#     s3_bucket = "udacity-dend",
#     s3_key = "song_data",
#     json_mode = "'auto'"
# )

load_songplays_table = LoadFactOperator(
    task_id = 'Load_songplays_fact_table',
    dag = dag,
    redshift_conn_id = "redshift",
    table = "songplays",
    insert_statement = SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id = 'Load_user_dim_table',
    redshift_conn_id = "redshift",
    table = "users",
    insert_statement = SqlQueries.user_table_insert,                                  
    append_mode = False,
    dag = dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id = 'Load_song_dim_table',
    redshift_conn_id = "redshift",
    table = "songs",
    insert_statement = SqlQueries.song_table_insert,                                  
    append_mode = False,
    dag = dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id = 'Load_artist_dim_table',
    redshift_conn_id = "redshift",
    table = "artists",
    insert_statement = SqlQueries.artist_table_insert,                                  
    append_mode = False,
    dag = dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id = 'Load_time_dim_table',
    redshift_conn_id = "redshift",
    table = "time",
    insert_statement = SqlQueries.time_table_insert,                                  
    append_mode = False,
    dag = dag
)

run_quality_checks = DataQualityOperator(
    task_id = 'Run_data_quality_checks',
    redshift_conn_id = "redshift",
    tables = [key for key, value in DataQuality.checks.items()], 
    quality_params = DataQuality.checks,
    dag = dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Scheduling tasks
start_task >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> [load_user_dimension_table,
                         load_song_dimension_table,
                         load_artist_dimension_table,
                         load_time_dimension_table] >> run_quality_checks
run_quality_checks >> end_operator
