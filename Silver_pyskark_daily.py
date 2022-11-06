import datetime
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
    dag_id='emr_job_flow_manual_steps_dag_silver_daily',
    schedule_interval=None,
    start_date=pendulum.datetime(2022, 1, 1, tz="UTC"),
    catchup=False,
    concurrency=10,
    max_active_runs=1
) as dag:

    #get date of yesterday
    now = datetime.datetime.now()
    #When the day is over, bring all of yesterday
    if now.strftime("%X") <= "00:20:00":
        date_t = date.today() - timedelta(1)
    else:
        date_t = date.today()

    #Thables name
    tables = ["inverters_data", "sites_invertory_details", "sites_metadata"]

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

    #Creates the cluster
    cluster_creator = EmrCreateJobFlowOperator(
        task_id='create_job_flow',
        job_flow_overrides=JOB_FLOW_OVERRIDES
    )

    #Terminated the cluster
    cluster_remover = EmrTerminateJobFlowOperator(
        task_id='remove_cluster',
        job_flow_id=cluster_creator.output, 
        trigger_rule = TriggerRule.ALL_DONE
    )

    #A loop of updates tables
    for table in tables:

        #Sends a pyspark script with a date variable to the cluster
        SPARK_STEPS = [
            {
                'Name': 'calculate_pi',
                'ActionOnFailure': 'CONTINUE',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': ["spark-submit","--deploy-mode","cluster",f"s3://pyspark-script/Silver-BrightSource_daily_{table}.py","--date_t",str(date_t)],
                },
            }
        ]

        #Runs an action with a pyspark script
        step_adder = EmrAddStepsOperator(
            task_id=f'add_steps_{table}',
            job_flow_id=cluster_creator.output,
            steps=SPARK_STEPS
        )

        #Waiting for the script to finish running
        step_checker = EmrStepSensor(
            task_id=f'watch_step_{table}',
            job_flow_id=cluster_creator.output,
            execution_timeout=timedelta(seconds=600),
            retries=0,
            retry_delay=timedelta(seconds=15),
            step_id=f"{{{{ task_instance.xcom_pull(task_ids='add_steps_{table}', key='return_value')[0] }}}}"
        )
        if table != "inverters_data_silver":
            
            #Deletes the old files from S3
            delete_hold_data = S3DeleteObjectsOperator(
                task_id=f"delete_hold_{table}_data",
                bucket="bse-silver",
                prefix=table,
            )

            delete_hold_data >> cluster_creator >> step_adder >> step_checker >> cluster_remover
    
        else:
            step_adder >> step_checker >> cluster_remover