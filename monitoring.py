import datetime
import pendulum
import time
import json
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
from datetime import timedelta, date
import re
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.email_operator import EmailOperator

from airflow import DAG
from airflow import AirflowException
from SolarEdge import Solaredge
from SolarEdgeAirFlowRunner import SolaredgeAirFlowRunner
from airflow.providers.amazon.aws.sensors.athena import AthenaSensor
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.operators.sql import SQLCheckOperator 
from airflow.exceptions import AirflowFailException

#Opens the file of the result of the query and receives an answer of true or false
def check_resutls(query_id):
    query_id = query_id.split('.')[0]
    import boto3
    client = boto3.client('athena')
    if client.get_query_results(QueryExecutionId=query_id)['ResultSet']['Rows'][1]['Data'][0]['VarCharValue'] == 'true':
        return True
    else:
        return AirflowFailException

with DAG(
    dag_id='monitoring',
    schedule_interval=None,
    start_date=pendulum.datetime(2022, 1, 1, tz="UTC"),
    catchup=False,
    concurrency=10,
    max_active_runs=1
) as dag:

    #dummy operators
    start = DummyOperator(task_id='start', dag=dag)
    end = DummyOperator(task_id='end', dag=dag, trigger_rule = TriggerRule.ALL_DONE)

    #Brings the dates of yesterday and two days ago
    yesterday_date = date.today() - timedelta(1)
    two_days_ago_date = date.today() - timedelta(2)

    #The emails to which notifications were sent
    emials = ["raz@jutomate.com", "omid@jutomate.com"]

    #A list of queries
    queries = [f"""
            SELECT count_a >= count_b
            FROM
            (SELECT count(distinct local_inverter_id) AS count_a
            FROM "brightsoutce_silver"."inverters_data"
            WHERE dt = '{yesterday_date}') as a,
            (SELECT count(distinct local_inverter_id) AS count_b
            FROM "brightsoutce_silver"."inverters_data"
            WHERE dt = '{two_days_ago_date}') as b 
           """,
           f"""
            SELECT count_a >= count_b
            FROM
            (SELECT count(distinct local_site_id) AS count_a
            FROM "brightsoutce_silver"."inverters_data"
            WHERE dt = '{yesterday_date}') as a,
            (SELECT count(distinct local_site_id) AS count_b
            FROM "brightsoutce_silver"."inverters_data"
            WHERE dt = '{two_days_ago_date}') as b
           """,
           f"""
            SELECT count(*) > 0
            FROM "brightsoutce_silver"."inverters_data"
            WHERE dt = '{yesterday_date}'
           """,
           f"""
            SELECT count(*) > 0
            FROM "brightsoutce_silver"."sites_invertory_details"
            WHERE dt = '{yesterday_date}'
           """,
           f"""
            SELECT count(*) > 0
            FROM "brightsoutce_silver"."sites_metadata"
            WHERE dt = '{yesterday_date}'
           """,
           f"""
            WITH
                t1 AS (SELECT count(*) as count_a FROM "brightsoutce_silver"."inverters_data"),
                t2 AS (SELECT count(*) as count_b from (SELECT distinct * FROM "brightsoutce_silver"."inverters_data"))
            SELECT count_a = count_b
            FROM t1, t2
           """,
           f"""
            WITH
                t1 AS (SELECT count(*) as count_a FROM "brightsoutce_silver"."sites_invertory_details"),
                t2 AS (SELECT count(*) as count_b from (SELECT distinct * FROM "brightsoutce_silver"."sites_invertory_details"))
            SELECT count_a = count_b
            FROM t1, t2
           """,
           f"""
            WITH
                t1 AS (SELECT count(*) as count_a FROM "brightsoutce_silver"."sites_metadata"),
                t2 AS (SELECT count(*) as count_b from (SELECT distinct * FROM "brightsoutce_silver"."sites_metadata"))
            SELECT count_a = count_b
            FROM t1, t2
           """
           ]

    #A list of error messages
    messages = ["There is less local_inverter_id yesterday than two days ago - inverters_data table", "There is less local_site_id yesterday than two days ago - inverters_data table", "The data was not updated yesterday - inverters_data table", "The data was not updated yesterday - sites_invertory_details table", "The data was not updated yesterday - sites_metadata table", "duplicate rows - inverters_data table", "duplicate rows - sites_invertory_details table" "duplicate rows - sites_metadata table"]
    
    #A loop that receives a query and a message separately
    for query, message, x in zip(queries, messages, range(len(queries))):
        #Sends a query to Athena and saves it in S3
        read_table = AthenaOperator(
            task_id=f'read_table{x}',
            query=query,
            database="brightsoutce_silver",
            output_location='s3://airflow-results/'
        )

        #Checks that an answer to the query has been received
        await_query = AthenaSensor(
            task_id=f'await_query{x}',
            query_execution_id=read_table.output,
        )
        
        #Run python operator that opens the file of the result of the query and receives an answer of true or false
        query_check = PythonOperator(
            task_id=f'query_check{x}',
            python_callable=check_resutls,
            op_kwargs={
                'query_id': read_table.output
            }
        )    
        
        #Sends an error to emails
        #Not working for now
        send_email = EmailOperator(
            task_id = f'send_email{x}',
            to = emials,
            subject = '[AIRFLOW] Error Message',
            html_content = message,
            conn_id=None,
            trigger_rule = TriggerRule.ALL_FAILED
        )

        start >> read_table >> await_query >> query_check >> send_email >> end
        