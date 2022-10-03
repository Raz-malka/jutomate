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

from airflow import DAG
from airflow import AirflowException
from SolarEdge import Solaredge
from SolarEdgeAirFlowRunner import SolaredgeAirFlowRunner

## get datetime from variable loop_start_date and loop_end_date (part of hisroty)
start_date  =   Variable.get("loop_start_date")
end_date    =   Variable.get("loop_end_date")
if (start_date is not None and  end_date is not None):
	start_date 	= datetime.datetime.strptime(start_date, '%Y-%m-%d')
	end_date 	= datetime.datetime.strptime(end_date, '%Y-%m-%d')

## get list of sites/ components
list_sites     = Variable.get("list_sites", deserialize_json=True)

# loop over date range
def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + timedelta(n)

## Brings the data by sites and by inverters
def update_inverters_data(datum_freq_min,site_id,serial_num,start_date,end_date):
    base_api = Solaredge("EC6PM19AGTOXAWM0QFTR5UFBH471V8O5","https://monitoringapi.solaredge.com")
    airflow_runner = SolaredgeAirFlowRunner(base_api,"bse-bronze")
    airflow_runner.get_invereter_data(str(site_id),serial_num,start_date,end_date)
    print("Thanks you for getting inverts list!")

with DAG(
    dag_id='get_all_data_history',
    schedule_interval=None,
    start_date=pendulum.datetime(2022, 1, 1, tz="UTC"),
    catchup=False,
    concurrency=10,
    max_active_runs=1
) as dag:

    #dummy operators
    start = DummyOperator(task_id='start', dag=dag)
    start_site_inverters_data = DummyOperator(task_id='start_site_inverters_data', dag=dag)
    end = DummyOperator(task_id='end', dag=dag, trigger_rule = TriggerRule.ALL_DONE)

    num_rows = 100
    dummy_count = 1
    rows_count = 0
    dummy_prev = DummyOperator(task_id=f'dummy_{dummy_count-1}', trigger_rule=TriggerRule.ALL_DONE, dag=dag)
    dummy_after = DummyOperator(task_id=f'dummy_{dummy_count}', trigger_rule=TriggerRule.ALL_DONE, dag=dag)

    start >> start_site_inverters_data >> dummy_prev

    ## loop that brings the list of inverters per site
    for site_id in list_sites:
        try:
            list_inverters = Variable.get("site_"+str(site_id)+"_inverters_list", deserialize_json=True)
        except:
            list_inverters = []
        # for each site - there is a list of inverters
        for inverter_id in list_inverters:
            # for each inverters - brings the range of dates
            for single_date in daterange(start_date, end_date):
                if single_date == datetime.datetime.now():
                    end_datetime = datetime.datetime.now()
                else:
                    end_datetime = single_date + datetime.timedelta(days=1)
                ## loop that brings the data of inverters and site
                update_data_task = PythonOperator(
                    task_id = 'update_inverters_list'+str(site_id)+str(inverter_id)+str(single_date.date()),
                    provide_context = True,
                    dag = dag,
                    op_kwargs = {
                                "datum_freq_min": 15,
                                "site_id": site_id,
                                "serial_num": inverter_id,
                                "start_date": start_date,
                                "end_date": end_datetime
                                },
                    python_callable = update_inverters_data, 
                    trigger_rule = TriggerRule.ALL_DONE)
                if rows_count == num_rows:
                    rows_count = 0
                    dummy_count += 1
                    dummy_prev = dummy_after
                    dummy_after = DummyOperator(task_id=f'dummy_{dummy_count}', trigger_rule=TriggerRule.ALL_DONE, dag=dag)
                dummy_prev >> update_data_task >> dummy_after
                rows_count +=1
    dummy_after >> end