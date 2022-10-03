from datetime import timedelta, date
import pendulum
from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator, EmrTerminateJobFlowOperator, EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectsOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.models import Variable

with DAG(
    dag_id='emr_job_flow_manual_steps_dag_gold_daily',
    schedule_interval=None,
    start_date=pendulum.datetime(2022, 1, 1, tz="UTC"),
    catchup=False,
    concurrency=10,
    max_active_runs=1
) as dag:

    #get date of yesterday
    date_t = date.today() - timedelta(1)

    #A query that updates the tables in athena
    queries = ["""
        MSCK REPAIR TABLE `brightsoutce_gold`.`inverters_data`;
    """,
    """
        MSCK REPAIR TABLE `brightsoutce_gold`.`inverters_data_agg`;
    """,
    """
        MSCK REPAIR TABLE `brightsoutce_gold`.`sites_metadata`;
    """]

    #Thables name
    tables = ["inverters_data", "inverters_data_agg", "sites_metadata"]

    #Sends a pyspark script with a date variable to the cluster
    SPARK_STEPS = [
        {
            'Name': 'calculate_pi',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': ["spark-submit","--deploy-mode","cluster","s3://pyspark-script/Gold-BrightSource_daily.py","--date_t",str(date_t)],
            },
        }
    ]

    #System data of the cluster
    JOB_FLOW_OVERRIDES = {
        'Name': 'pyspark',
        'ReleaseLabel': 'emr-5.29.0',
        'LogUri': 's3://aws-logs-479886561928-eu-west-2/elasticmapreduce/',
        'Applications': [{'Name': 'Spark'}],
        'Instances': {
            'InstanceGroups': [
                {
                    'Name': 'Master node',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': 'm5.xlarge',
                    'InstanceCount': 1,
                }
            ],
            'KeepJobFlowAliveWhenNoSteps': True, #keep cluster alive after step is done
            'TerminationProtected': False,
        },
        'JobFlowRole': 'EMR_EC2_DefaultRole',
        'ServiceRole': 'EMR_DefaultRole',
    }

    #Deletes the old files from S3 of sites metadata
    delete_hold_sites_metadata_data = S3DeleteObjectsOperator(
        task_id="delete_hold_sites_metadata_data",
        bucket="bse-gold",
        prefix="sites_metadata",
    )

    #Creates the cluster
    cluster_creator = EmrCreateJobFlowOperator(
        task_id='create_job_flow',
        job_flow_overrides=JOB_FLOW_OVERRIDES
    )

    #Runs an action with a pyspark script
    step_adder = EmrAddStepsOperator(
        task_id='add_steps',
        job_flow_id=cluster_creator.output,
        steps=SPARK_STEPS
    )

    #Waiting for the script to finish running
    step_checker = EmrStepSensor(
        task_id='watch_step',
        job_flow_id=cluster_creator.output,
        step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')[0] }}"
    )

    #Terminated the cluster
    cluster_remover = EmrTerminateJobFlowOperator(
        task_id='remove_cluster',
        job_flow_id=cluster_creator.output, 
        trigger_rule = TriggerRule.ALL_DONE
    )
    
    #Running the monitoring dag
    run_monitoring_daily = TriggerDagRunOperator(
            task_id='run_monitoring_daily',
            trigger_dag_id='monitoring',
            wait_for_completion=True,
            trigger_rule=TriggerRule.ALL_DONE
        )

    delete_hold_sites_metadata_data >> cluster_creator
    step_adder >> step_checker >> cluster_remover

    #A loop of updates tables
    for query, table in zip(queries, tables):
        Updating_tables = AthenaOperator(
            task_id=f'Updating_tables_{table}',
            query=query,
            database="brightsoutce_silver",
            output_location='s3://airflow-results/'
        )

        cluster_remover >> Updating_tables >> run_monitoring_daily