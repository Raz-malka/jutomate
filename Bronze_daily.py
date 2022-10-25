from datetime import timedelta, date
import pendulum
from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator, EmrTerminateJobFlowOperator, EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectsOperator
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from sqlalchemy import table

with DAG(
    dag_id='bronze_daily',
    schedule_interval='0 4 * * *',
    start_date=pendulum.datetime(2022, 1, 1, tz="UTC"),
    catchup=False,
    concurrency=10,
    max_active_runs=1
) as dag:

    #A query that updates the tables in athena
    queries = ["""
        MSCK REPAIR TABLE `brightsoutce_bronze`.`inverters_data`;
    """,
    """
        MSCK REPAIR TABLE `brightsoutce_bronze`.`sites_invertory_details`;
    """,
    """
        MSCK REPAIR TABLE `brightsoutce_bronze`.`sites_metadata`;
    """]

    #Thables name
    tables = ["inverters_data_bronze", "sites_invertory_details_bronze", "sites_metadata_bronze"]
    
    #A loop of updates tables
    for query, table in zip(queries, tables):

        Updating_tables = AthenaOperator(
            task_id=f'Updating_tables_{table}',
            query=query,
            database="brightsoutce_silver",
            output_location='s3://airflow-results/'
        )