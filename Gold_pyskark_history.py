from datetime import timedelta
import datetime
import pendulum
from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator, EmrTerminateJobFlowOperator, EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.models import Variable

# loop over date range
def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + timedelta(n)

with DAG(
    dag_id='emr_job_flow_manual_steps_dag_gold_history',
    schedule_interval=None,
    start_date=pendulum.datetime(2022, 1, 1, tz="UTC"),
    catchup=False,
    concurrency=10,
    max_active_runs=1
) as dag:

    ## get datetime from variable loop_start_date and loop_end_date (part of hisroty)
    start_date = Variable.get("history_loop_start_date_pyspark")
    end_date = Variable.get("history_loop_end_date_pyspark")
    if (start_date is not None and  end_date is not None):
        start_date 	= datetime.datetime.strptime(start_date, '%Y-%m-%d').date()
        end_date 	= datetime.datetime.strptime(end_date, '%Y-%m-%d').date()

    #A query that updates the tables in athena
    queries = ["""
        MSCK REPAIR TABLE `brightsource_gold`.`inverters_data`;
    """,
    """
        MSCK REPAIR TABLE `brightsource_gold`.`inverters_data_agg`;
    """]

    #Thables name
    tables = ["inverters_data", "inverters_data_agg"]

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

    #A loop of dates
    for date_t in daterange(start_date, end_date):

        #Sends a pyspark script with a date variable to the cluster
        SPARK_STEPS = [
            {
                'Name': 'calculate_pi',
                'ActionOnFailure': 'CONTINUE',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': ["spark-submit","--deploy-mode","cluster","s3://pyspark-script/Gold-BrightSource_history.py","--date_t",str(date_t)],
                },
            }
        ]

        #Runs an action with a pyspark script
        step_adder = EmrAddStepsOperator(
            task_id=f'add_steps_{date_t}',
            job_flow_id=cluster_creator.output,
            steps=SPARK_STEPS
        )

        #Waiting for the script to finish running
        step_checker = EmrStepSensor(
            task_id=f'watch_step_{date_t}',
            job_flow_id=cluster_creator.output,
            step_id=f"{{{{ task_instance.xcom_pull(task_ids='add_steps_{date_t}', key='return_value')[0] }}}}"
        )

        step_adder >> step_checker >> cluster_remover

    #A loop of updates tables
    for query, table in zip(queries, tables):
        Updating_tables = AthenaOperator(
            task_id=f'Updating_tables_{table}',
            query=query,
            database="brightsource_silver",
            output_location='s3://airflow-results/'
        )

        cluster_remover >> Updating_tables