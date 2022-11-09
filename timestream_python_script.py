import pendulum
from airflow import DAG
import boto3
import pandas as pd
import io
import datetime
from datetime import timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.dummy_operator import DummyOperator

# loop over date range
def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + timedelta(n)

def timestream_gold_writing(single_date):
    s3 = boto3.client('s3')
    client_s3 = boto3.resource('s3')
    paginator = s3.get_paginator('list_objects')
    client_timestream = boto3.client('timestream-write', region_name='eu-west-1')
    page_iterator = paginator.paginate(Bucket='bse-gold',Prefix='inverters_data/dt='+str(single_date))

    for page in page_iterator:
        
        all_objects = page['Contents']
        
        for obj in all_objects:
            
            file_name = obj['Key']
            
            if file_name.endswith('parquet'):
                
                buffer = io.BytesIO()
                object = client_s3.Object('bse-gold', file_name)
                object.download_fileobj(buffer)
                df = pd.read_parquet(buffer)
                df = df.replace('','0')
                
                for x in range(len(df.axes[0])):
                    
                    dimensions = [{'Name': 'data_source', 'Value': str(df.Data_Source[x])}, {'Name': 'local_site_id','Value': str(df.Local_site_id[x])}, {'Name': 'local_inverter_id','Value': str(df.Local_inverter_id[x])}]
                    date_time = datetime.datetime.strptime(df.Datetime[x], '%Y-%m-%d %H:%M:%S').strftime('%s')
                    timestamp_millisecond = date_time + "000"
                    
                    common_attributes = {
                        'Dimensions': dimensions,
                        'MeasureValueType': 'DOUBLE',
                        'Time': str(timestamp_millisecond)
                    }
                    
                    dc_voltage = {'MeasureName': 'dc_voltage','MeasureValue': str(df.DC_Voltage[x])}
                    temperature = {'MeasureName': 'temperature','MeasureValue': str(df.Temperature[x])}
                    active_power = {'MeasureName': 'active_power','MeasureValue': str(df.Active_Power[x])}
                    ac_energy = {'MeasureName': 'ac_energy','MeasureValue': str(df.AC_Energy[x])}
                    ac_current_l1 = {'MeasureName': 'ac_current_l1','MeasureValue': str(df.AC_Current_L1[x])}
                    ac_voltage_l1 = {'MeasureName': 'ac_voltage_l1','MeasureValue': str(df.AC_Voltage_L1[x])}
                    ac_current_l2 = {'MeasureName': 'ac_current_l2','MeasureValue': str(df.AC_Current_L2[x])}
                    ac_voltage_l2 = {'MeasureName': 'ac_voltage_l2','MeasureValue': str(df.AC_Voltage_L2[x])}
                    ac_current_l3 = {'MeasureName': 'ac_current_l3','MeasureValue': str(df.AC_Current_L3[x])}
                    ac_voltage_l3 = {'MeasureName': 'ac_voltage_l3','MeasureValue': str(df.AC_Voltage_L3[x])}
                    ac_current = {'MeasureName': 'ac_current','MeasureValue': str(df.AC_Current[x])}
                    ac_voltage = {'MeasureName': 'ac_voltage','MeasureValue': str(df.AC_Voltage[x])}
                    reactive_power = {'MeasureName': 'reactive_power','MeasureValue': str(df.Reactive_Power[x])}
                    power_factor = {'MeasureName': 'power_factor','MeasureValue': str(df.Power_Factor[x])}
                    ac_power = {'MeasureName': 'ac_power','MeasureValue': str(df.AC_Power[x])}
                    dc_current = {'MeasureName': 'dc_current','MeasureValue': str(df.DC_Current[x])}
                    dc_power = {'MeasureName': 'dc_power','MeasureValue': str(df.DC_Power[x])}
                    account_id = {'MeasureName': 'account_id','MeasureValue': str(df.Account_id[x])}
                    
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
                    
with DAG(
    dag_id='timestream',
    schedule_interval=None,
    start_date=pendulum.datetime(2022, 1, 1, tz="UTC"),
    catchup=False,
    concurrency=10,
    max_active_runs=1
) as dag:
    
    start_date = "2022-02-05"
    end_date = "2022-11-01"
    if (start_date is not None and  end_date is not None):
        start_date 	= datetime.datetime.strptime(start_date, '%Y-%m-%d').date()
        end_date 	= datetime.datetime.strptime(end_date, '%Y-%m-%d').date()
    
    num_rows = 1
    dummy_count = 1
    rows_count = 0
    dummy_prev_site = DummyOperator(task_id=f'dummy_site_{dummy_count - 1}', trigger_rule=TriggerRule.ALL_DONE, dag=dag)
    dummy_after_site = DummyOperator(task_id=f'dummy_site_{dummy_count}', trigger_rule=TriggerRule.ALL_DONE, dag=dag)
    for single_date in daterange(start_date, end_date):

        update_data_timestream = PythonOperator(
            task_id='update_data_timestream_'+str(single_date),
            provide_context=True,
            dag=dag,
            op_kwargs={
                        "single_date": single_date
                    },
            python_callable=timestream_gold_writing, 
            trigger_rule=TriggerRule.ALL_DONE)
        if rows_count == num_rows:
            rows_count = 0
            dummy_count += 1
            dummy_prev_site = dummy_after_site
            dummy_after_site = DummyOperator(task_id=f'dummy_site_{dummy_count}', trigger_rule=TriggerRule.ALL_DONE, dag=dag)    
        dummy_prev_site >> update_data_timestream >> dummy_after_site
        rows_count = rows_count + 1