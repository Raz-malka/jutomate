import datetime
from datetime import timedelta, date
import pandas as pd
import io
import boto3
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
    
def timestream_gold_writing(date_t):
    s3 = boto3.client('s3')
    client_s3 = boto3.resource('s3')
    paginator = s3.get_paginator('list_objects')
    client_timestream = boto3.client('timestream-write', region_name='eu-west-1')
    page_iterator = paginator.paginate(Bucket='bse-gold',Prefix='inverters_data/dt='+str(date_t))

    for page in page_iterator:
        
        all_objects = page['Contents']
        
        for obj in all_objects:
            
            file_name = obj['Key']
            
            if file_name.endswith('parquet'):
                
                buffer = io.BytesIO()
                object = client_s3.Object('bse-gold', file_name)
                object.download_fileobj(buffer)
                df = pd.read_parquet(buffer)
                df = df.fillna(0)
                
                for x in range(len(df.axes[0])):
                    
                    dimensions = [{'Name': 'data_source', 'Value': str(df.Data_Source[x])}, {'Name': 'local_site_id','Value': str(df.Local_site_id[x])}, {'Name': 'local_inverter_id','Value': str(df.Local_inverter_id[x])}]
                    date_time = datetime.datetime.strptime(df.Datetime[x], '%Y-%m-%d %H:%M:%S').strftime('%s')
                    timestamp_millisecond = date_time + "000"
                    
                    common_attributes = {
                        'Dimensions': dimensions,
                        'MeasureValueType': 'DOUBLE',
                        'Time': str(timestamp_millisecond)
                    }
                    
                    dc_voltage = {'MeasureName': 'dc_voltage','MeasureValue': str(round(float(df.DC_Voltage[x]),3))}
                    temperature = {'MeasureName': 'temperature','MeasureValue': str(round(float(df.Temperature[x]),3))}
                    active_power = {'MeasureName': 'active_power','MeasureValue': str(round(float(df.Active_Power[x]),3))}
                    ac_energy = {'MeasureName': 'ac_energy','MeasureValue': str(round(float(df.AC_Energy[x]),3))}
                    ac_current_l1 = {'MeasureName': 'ac_current_l1','MeasureValue': str(round(float(df.AC_Current_L1[x]),3))}
                    ac_voltage_l1 = {'MeasureName': 'ac_voltage_l1','MeasureValue': str(round(float(df.AC_Voltage_L1[x]),3))}
                    ac_current_l2 = {'MeasureName': 'ac_current_l2','MeasureValue': str(round(float(df.AC_Current_L2[x]),3))}
                    ac_voltage_l2 = {'MeasureName': 'ac_voltage_l2','MeasureValue': str(round(float(df.AC_Voltage_L2[x]),3))}
                    ac_current_l3 = {'MeasureName': 'ac_current_l3','MeasureValue': str(round(float(df.AC_Current_L3[x]),3))}
                    ac_voltage_l3 = {'MeasureName': 'ac_voltage_l3','MeasureValue': str(round(float(df.AC_Voltage_L3[x]),3))}
                    ac_current = {'MeasureName': 'ac_current','MeasureValue': str(round(float(df.AC_Current[x]),3))}
                    ac_voltage = {'MeasureName': 'ac_voltage','MeasureValue': str(round(float(df.AC_Voltage[x]),3))}
                    reactive_power = {'MeasureName': 'reactive_power','MeasureValue': str(round(float(df.Reactive_Power[x]),3))}
                    power_factor = {'MeasureName': 'power_factor','MeasureValue': str(round(float(df.Power_Factor[x]),3))}
                    ac_power = {'MeasureName': 'ac_power','MeasureValue': str(round(float(df.AC_Power[x]),3))}
                    dc_current = {'MeasureName': 'dc_current','MeasureValue': str(round(float(df.DC_Current[x]),3))}
                    dc_power = {'MeasureName': 'dc_power','MeasureValue': str(round(float(df.DC_Power[x]),3))}
                    account_id = {'MeasureName': 'account_id','MeasureValue': str(round(float(df.Account_id[x]),3))}
                    
                    records = [dc_voltage, temperature, active_power, ac_energy, ac_current_l1, ac_voltage_l1, ac_current_l2, ac_voltage_l2, ac_current_l3, ac_voltage_l3, ac_current, ac_voltage, reactive_power, power_factor, ac_power, dc_current, dc_power, account_id]
                    
                    try:    
                        result = client_timestream.write_records(DatabaseName="brightsource_gold", TableName="inverters_data", Records=records, CommonAttributes=common_attributes)
                    
                    except Exception as err:
                            print("Error:", err)
                            print("""


                            """)
                            print(err.response)
                            print("""

                            
                            """)
                            print(err.response["Error"])

                    print("WriteRecords Status: [%s]" % result['ResponseMetadata']['HTTPStatusCode'])

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

    update_data_timestream = PythonOperator(
        task_id='update_data_timestream_'+str(date_t),
        provide_context=True,
        dag=dag,
        op_kwargs={
                    "date_t": date_t
                },
        python_callable=timestream_gold_writing, 
        trigger_rule=TriggerRule.ONE_SUCCESS)

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

                step_checker_inverters_data_silver >> step_adder_inverters_data_gold >> step_checker_inverters_data_gold >> cluster_remover >> update_date_variable
                step_checker_inverters_data_gold >> update_data_timestream >> update_date_variable
            
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

                delete_hold_data_sites_metadata_silver >> step_adder_inverters_data__sites_metadatagold >> step_checker_inverters_data__sites_metadatagold >> delete_hold_sites_metadata_data_gold >> cluster_remover