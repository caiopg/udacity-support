from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from airflow.secrets.metastore import MetastoreBackend
from airflow.models import Variable
from udacity.common import final_project_sql_statements
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator


default_args = {
    'owner': 'udacity',
    'start_date': pendulum.now(),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'depends_on_past': False
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@hourly',
    catchup=False
)
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')

    metastore_backend = MetastoreBackend()
    aws_conn = metastore_backend.get_connection("aws_credentials")

    bucket_name = Variable.get("s3_bucket")
    events_s3_path_with_bucket = "s3://" + bucket_name + "/log-data/{{ execution_date.year }}/{{ execution_date.month }}/{{ execution_date.year }}-{{ execution_date.month }}-{{ execution_date.day }}-events.json"

    events_json_format_with_bucket = f"s3://{bucket_name}/log_json_path.json"

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        # aws_access_key_id=aws_conn.login,
        # aws_secret_access_key=aws_conn.password, 
        # postgres_conn_id = "redshift",
        table='staging_events',
        # s3_path = events_s3_path_with_bucket,
        # region = "us-east-1",
        # json_format = events_json_format_with_bucket
    )
    
    songs_s3_path_with_bucket = f"s3://{bucket_name}/song-data/"

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        # aws_access_key_id=aws_conn.login,
        # aws_secret_access_key=aws_conn.password,
        # postgres_conn_id = "redshift",
        table='staging_songs',
        # s3_path = songs_s3_path_with_bucket,
        # region = "us-east-1",
        # json_format = "auto"
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
    )

    start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]
    [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
    load_songplays_table >> [load_artist_dimension_table, load_song_dimension_table, load_time_dimension_table, load_user_dimension_table]
    [load_artist_dimension_table, load_song_dimension_table, load_time_dimension_table, load_user_dimension_table] >> run_quality_checks


final_project_dag = final_project()
