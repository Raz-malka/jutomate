import datetime
from datetime import timedelta, date
import pendulum
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator, EmrTerminateJobFlowOperator, EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectsOperator
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.models import Variable
from sqlalchemy import table

def dateset():
    Variable.set("today", date.today())

## get datetime of now and the start of the day
date_t          = date.today()
airflow_date = Variable.get("today")
airflow_date = datetime.datetime.strptime(airflow_date, '%Y-%m-%d').date()
## if it new day bring the start of yesterday and start of today 
if date_t != airflow_date:
    date_t = date_t - timedelta(days=1)

with DAG(
    dag_id='emr_job_flow_manual_steps_dag_silver_gold_daily',
    schedule_interval=None,
    start_date=pendulum.datetime(2022, 1, 1, tz="UTC"),
    catchup=False,
    concurrency=10,
    max_active_runs=1
) as dag:

    update_date_variable = PythonOperator(
        task_id='update_date_variable',
        provide_context=True,
        dag=dag,
        python_callable=dateset, 
        trigger_rule=TriggerRule.ALL_DONE)

    #Thables name
    tables = ["inverters_data_silver", "sites_invertory_details_silver", "sites_metadata_silver", "inverters_data_gold", "inverters_data_agg_gold", "sites_metadata_gold"]

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
                    'InstanceType': 'm5.4xlarge',
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
        #if the table is silver
        if table.endswith('_silver'):
            #get the name without _silver
            table_name = table.replace('_silver', '')

            #Sends a pyspark script with a date variable to the cluster
            SPARK_STEPS_SILVER = [
                {
                    'Name': 'calculate_pi',
                    'ActionOnFailure': 'CONTINUE',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ["spark-submit","--deploy-mode","cluster",f"s3://pyspark-script/Silver-BrightSource_daily_{table_name}.py","--date_t",str(date_t)],
                    },
                }
            ]
            #Separator between the tables
            if table_name == "inverters_data":

                #Runs an action with a pyspark script
                step_adder_inverters_data_silver = EmrAddStepsOperator(
                    task_id=f'add_steps_{table}',
                    job_flow_id=cluster_creator.output,
                    steps=SPARK_STEPS_SILVER
                )

                #Waiting for the script to finish running
                step_checker_inverters_data_silver = EmrStepSensor(
                    task_id=f'watch_step_{table}',
                    job_flow_id=cluster_creator.output,
                    execution_timeout=timedelta(seconds=1200),
                    retries=0,
                    retry_delay=timedelta(seconds=15),
                    step_id=f"{{{{ task_instance.xcom_pull(task_ids='add_steps_{table}', key='return_value')[0] }}}}"
                )

                step_adder_inverters_data_silver >> step_checker_inverters_data_silver

            elif table_name == "sites_metadata":

                #Runs an action with a pyspark script
                step_adder_sites_metadata_silver = EmrAddStepsOperator(
                    task_id=f'add_steps_{table}',
                    job_flow_id=cluster_creator.output,
                    steps=SPARK_STEPS_SILVER
                )

                #Waiting for the script to finish running
                step_checker_sites_metadata_silver = EmrStepSensor(
                    task_id=f'watch_step_{table}',
                    job_flow_id=cluster_creator.output,
                    execution_timeout=timedelta(seconds=1200),
                    retries=0,
                    retry_delay=timedelta(seconds=15),
                    step_id=f"{{{{ task_instance.xcom_pull(task_ids='add_steps_{table}', key='return_value')[0] }}}}"
                )
            
                #Deletes the old files from S3
                delete_hold_data_sites_metadata_silver = S3DeleteObjectsOperator(
                    task_id=f"delete_hold_{table}_data",
                    bucket="bse-silver",
                    prefix=table_name +"/dt="+ str(date_t - timedelta(1)),
                )

                step_adder_sites_metadata_silver >> step_checker_sites_metadata_silver >> delete_hold_data_sites_metadata_silver

            else:

                #Runs an action with a pyspark script
                step_adder_sites_invertory_details_silver = EmrAddStepsOperator(
                    task_id=f'add_steps_{table}',
                    job_flow_id=cluster_creator.output,
                    steps=SPARK_STEPS_SILVER
                )

                #Waiting for the script to finish running
                step_checker_sites_invertory_details_silver = EmrStepSensor(
                    task_id=f'watch_step_{table}',
                    job_flow_id=cluster_creator.output,
                    execution_timeout=timedelta(seconds=1200),
                    retries=0,
                    retry_delay=timedelta(seconds=15),
                    step_id=f"{{{{ task_instance.xcom_pull(task_ids='add_steps_{table}', key='return_value')[0] }}}}"
                )
            
                #Deletes the old files from S3
                delete_hold_data_sites_invertory_details_silver = S3DeleteObjectsOperator(
                    task_id=f"delete_hold_{table}_data",
                    bucket="bse-silver",
                    prefix=table_name +"/dt="+ str(date_t - timedelta(1)),
                )

                step_adder_sites_invertory_details_silver >> step_checker_sites_invertory_details_silver >> delete_hold_data_sites_invertory_details_silver
        
        else:
            #get the name without _silver
            table_name = table.replace('_gold', '')

            #Sends a pyspark script with a date variable to the cluster
            SPARK_STEPS_GOLD = [
                {
                    'Name': 'calculate_pi',
                    'ActionOnFailure': 'CONTINUE',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ["spark-submit","--deploy-mode","cluster",f"s3://pyspark-script/Gold-BrightSource_daily_{table_name}.py","--date_t",str(date_t)],
                    },
                }
            ]

            #Separator between the tables
            if table_name.startswith("inverters_data"):

                #Runs an action with a pyspark script
                step_adder_inverters_data_gold = EmrAddStepsOperator(
                    task_id=f'add_steps_{table}',
                    job_flow_id=cluster_creator.output,
                    steps=SPARK_STEPS_GOLD
                )

                #Waiting for the script to finish running
                step_checker_inverters_data_gold = EmrStepSensor(
                    task_id=f'watch_step_{table}',
                    job_flow_id=cluster_creator.output,
                    execution_timeout=timedelta(seconds=1200),
                    retries=0,
                    retry_delay=timedelta(seconds=15),
                    step_id=f"{{{{ task_instance.xcom_pull(task_ids='add_steps_{table}', key='return_value')[0] }}}}"
                )

                step_checker_inverters_data_silver >> step_adder_inverters_data_gold >> step_checker_inverters_data_gold >> update_date_variable >> cluster_remover
            
            else:

                #Runs an action with a pyspark script
                step_adder_inverters_data__sites_metadatagold = EmrAddStepsOperator(
                    task_id=f'add_steps_{table}',
                    job_flow_id=cluster_creator.output,
                    steps=SPARK_STEPS_GOLD
                )

                #Waiting for the script to finish running
                step_checker_inverters_data__sites_metadatagold = EmrStepSensor(
                    task_id=f'watch_step_{table}',
                    job_flow_id=cluster_creator.output,
                    execution_timeout=timedelta(seconds=1200),
                    retries=0,
                    retry_delay=timedelta(seconds=15),
                    step_id=f"{{{{ task_instance.xcom_pull(task_ids='add_steps_{table}', key='return_value')[0] }}}}"
                )
                
                #Deletes the old files from S3 of sites metadata
                delete_hold_sites_metadata_data_gold = S3DeleteObjectsOperator(
                    task_id=f"delete_hold_{table}_data",
                    bucket="bse-gold",
                    prefix=table_name +"/dt="+ str(date_t - timedelta(1)),
                )

                delete_hold_data_sites_metadata_silver >> step_adder_inverters_data__sites_metadatagold >> step_checker_inverters_data__sites_metadatagold >> delete_hold_sites_metadata_data_gold >> update_date_variable >> cluster_remover