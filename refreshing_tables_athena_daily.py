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
    dag_id='refreshing_tables_athena_daily',
    schedule_interval='0 4 * * *',
    start_date=pendulum.datetime(2022, 1, 1, tz="UTC"),
    catchup=False,
    concurrency=10,
    max_active_runs=1
) as dag:

    #A query that updates the tables in athena
    queries = ["""
        MSCK REPAIR TABLE `brightsource_bronze`.`sites_invertory_details`;
    """,
    """
        MSCK REPAIR TABLE `brightsource_bronze`.`sites_metadata`;
    """,
    """
        MSCK REPAIR TABLE `brightsource_silver`.`inverters_data`;
    """,
    """
        MSCK REPAIR TABLE `brightsource_silver`.`sites_invertory_details`;
    """,
    """
        MSCK REPAIR TABLE `brightsource_silver`.`sites_metadata`;
    """,
    """
        MSCK REPAIR TABLE `brightsource_gold`.`inverters_data`;
    """,
    """
        MSCK REPAIR TABLE `brightsource_gold`.`inverters_data_agg`;
    """,
    """
        MSCK REPAIR TABLE `brightsource_gold`.`sites_metadata`;
    """]

    #Thables name
    tables = ["sites_invertory_details_bronze", "sites_metadata_bronze","inverters_data_silver", "sites_invertory_details_silver", "sites_metadata_silver","inverters_data", "inverters_data_agg", "sites_metadata"]
    
    #A loop of updates tables
    for query, table in zip(queries, tables):

        if table.endswith('bronze'):
            database_name = 'bronze'
        if table.endswith('silver'):
            database_name = 'silver'
        if table.endswith('gold'):
            database_name = 'gold'
        Updating_tables = AthenaOperator(
            task_id=f'Updating_tables_{table}',
            query=query,
            database=f"brightsource_{database_name}",
            output_location='s3://airflow-results/'
        )