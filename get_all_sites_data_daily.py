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

## get datetime of now and the start of the day
start_date  =   datetime.datetime.combine(datetime.date.today(), datetime.datetime.min.time())
end_date    =   datetime.datetime.now()
## if it new day bring the start of yesterday and start of today 
if end_date.strftime("%X") <= "00:20:00":
    end_date = start_date
    start_date = start_date - timedelta(days=1)

## get list of sites/ components
list_sites     = Variable.get("list_sites", deserialize_json=True)

## brings the list of sites
def update_site_list(datum_freq_min):
    base_api = Solaredge("EC6PM19AGTOXAWM0QFTR5UFBH471V8O5","https://monitoringapi.solaredge.com")
    airflow_runner = SolaredgeAirFlowRunner(base_api,"bse-bronze")
    # get site list
    sites = airflow_runner.get_sites()
    site_id_list = []
    for site_dict in sites['sites']['site']:
        site_id_list.append(site_dict.get('id'))
    #update airflow variable
    print(site_id_list)
    Variable.set("list_sites", site_id_list)
    print("Thanks you for getting sites list!")

## brings the list of inverters
def update_inverters_list(datum_freq_min,site_id):
    base_api = Solaredge("EC6PM19AGTOXAWM0QFTR5UFBH471V8O5","https://monitoringapi.solaredge.com")
    airflow_runner = SolaredgeAirFlowRunner(base_api,"bse-bronze")
    # get inverter list
    inventer_id_list = []
    inventory = airflow_runner.get_inventory(str(site_id))
    inverters = inventory.get('Inventory').get('inverters')
    for inverter in inverters:
        #single inverter
        print (inverter.get('SN'))
        inventer_id_list.append(inverter.get('SN'))
    Variable.set("site_"+str(site_id)+"_inverters_list",json.dumps(inventer_id_list))
    print("Thanks you for getting inverts list!")

## Brings the data by sites and by inverters
def update_inverters_data(datum_freq_min,site_id,serial_num,start_date,end_date):
    base_api = Solaredge("EC6PM19AGTOXAWM0QFTR5UFBH471V8O5","https://monitoringapi.solaredge.com")
    airflow_runner = SolaredgeAirFlowRunner(base_api,"bse-bronze")
    invereter_data = airflow_runner.get_invereter_data(str(site_id),serial_num,start_date,end_date)
    print("invereter_data:")
    print(invereter_data)
    print("Thanks you for getting inverts list!")

with DAG(
    dag_id='get_all_sites_data_daily',
    schedule_interval=None,
    start_date=pendulum.datetime(2022, 1, 1, tz="UTC"),
    catchup=False,
    concurrency=20,
    max_active_runs=1
) as dag:

    #dummy operators
    start = DummyOperator(task_id='start', dag=dag)
    start_site_inverters_loop = DummyOperator(task_id='start_site_inverters_loop', dag=dag)
    start_site_inverters_data = DummyOperator(task_id='start_site_inverters_data', dag=dag)
    end = DummyOperator(task_id='end', dag=dag, trigger_rule = TriggerRule.ONE_SUCCESS)

    #example : 132 sites, 1-4 inverters per site, each inveter has componenets.
    update_site_list_task = PythonOperator(
        task_id='update_site_list_task',
        provide_context=True,
        dag=dag,
        op_kwargs={
                    "datum_freq_min": 15
                  },
        python_callable=update_site_list, 
        trigger_rule=TriggerRule.ALL_DONE)
    start >> update_site_list_task >> start_site_inverters_loop

    num_rows = 100
    dummy_count = 1
    rows_count = 0
    dummy_prev_site = DummyOperator(task_id=f'dummy_site_{dummy_count - 1}', trigger_rule=TriggerRule.ALL_DONE, dag=dag)
    dummy_after_site = DummyOperator(task_id=f'dummy_site_{dummy_count}', trigger_rule=TriggerRule.ALL_DONE, dag=dag)

    start_site_inverters_loop >> dummy_prev_site

    ## loop that brings the list of inverters per site
    for site_id in list_sites:
        # for each site - there is a list of inverters
        update_inverters_list_task = PythonOperator(
            task_id = 'update_inverters_list_'+str(site_id),
            provide_context = True,
            dag = dag,
            op_kwargs = {
                            "datum_freq_min": 15,
                            "site_id": site_id
                        },
            execution_timeout=timedelta(seconds=600),
            retries=2,
            retry_delay=timedelta(seconds=10),
            python_callable = update_inverters_list, 
            trigger_rule = TriggerRule.ALL_DONE)
        if rows_count == num_rows:
            rows_count = 0
            dummy_count += 1
            dummy_prev_site = dummy_after_site
            dummy_after_site = DummyOperator(task_id=f'dummy_site_{dummy_count}', trigger_rule=TriggerRule.ALL_DONE, dag=dag)    
        dummy_prev_site >> update_inverters_list_task >> dummy_after_site
        rows_count = rows_count + 1

    num_rows = 100
    dummy_count = 1
    rows_count = 0
    dummy_prev_inverters = DummyOperator(task_id=f'dummy_inverters_{dummy_count - 1}', trigger_rule=TriggerRule.ALL_DONE, dag=dag)
    dummy_after_inverters = DummyOperator(task_id=f'dummy_inverters_{dummy_count}', trigger_rule=TriggerRule.ALL_DONE, dag=dag)

    dummy_after_site >> start_site_inverters_data >> dummy_prev_inverters

    for site_id in list_sites:
        try:
            list_inverters = Variable.get("site_"+str(site_id)+"_inverters_list", deserialize_json=True)
        except:
            list_inverters = []
        for inverter_id in list_inverters:
            ## loop that brings the data of inverters and site
            update_inverters_data_task = PythonOperator(
                task_id = 'update_inverters_list_'+str(site_id)+'_'+str(inverter_id)+'_'+str(start_date.date()),
                provide_context = True,
                dag = dag,
                op_kwargs = {
                            "datum_freq_min": 15,
                            "site_id": site_id,
                            "serial_num": inverter_id,
                            "start_date": start_date,
                            "end_date": end_date
                            },
                execution_timeout=timedelta(seconds=600),
                retries=2,
                retry_delay=timedelta(seconds=10),
                python_callable = update_inverters_data, 
                trigger_rule = TriggerRule.ALL_DONE)
            if rows_count == num_rows:
                rows_count = 0
                dummy_count += 1
                dummy_prev_inverters = dummy_after_inverters
                dummy_after_inverters = DummyOperator(task_id=f'dummy_inverters_{dummy_count}', trigger_rule=TriggerRule.ALL_DONE, dag=dag)    
            dummy_prev_inverters >> update_inverters_data_task >> dummy_after_inverters
            rows_count = rows_count + 1
    dummy_after_inverters >> end