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
    dag_id='daily_trigger',
    schedule_interval='0 * * * *',
    start_date=pendulum.datetime(2022, 1, 1, tz="UTC"),
    catchup=False,
    concurrency=10,
    max_active_runs=1
) as dag:

    #Running the sites data daily dag
    run_get_all_sites_data_daily = TriggerDagRunOperator(
            task_id='run_get_all_sites_data_daily',
            trigger_dag_id='get_all_sites_data_daily',
            wait_for_completion=True,
            execution_timeout=timedelta(seconds=3000)
        )

    #Running the silver and gold daily dag
    run_silver_gold_daily = TriggerDagRunOperator(
            task_id='run_silver_gold_daily',
            trigger_dag_id='emr_job_flow_manual_steps_dag_silver_gold_daily',
            wait_for_completion=True,
            execution_timeout=timedelta(seconds=3000)
        )

    #Running the monitoring dag
    # run_monitoring = TriggerDagRunOperator(
    #         task_id='run_monitoring',
    #         trigger_dag_id='monitoring',
    #         wait_for_completion=True
    #     )
    
    run_get_all_sites_data_daily >> run_silver_gold_daily #>> run_monitoring