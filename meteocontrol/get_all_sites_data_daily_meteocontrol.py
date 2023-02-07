import datetime
import pendulum
import time
from time import sleep
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
from meteocontrol.trackingutils import PositionControl
from meteocontrol.datesutil import Dates
from meteocontrol.Meteocontrol import Meteocontrol
from meteocontrol.meteocontrol_airflow_runner import MeteocontrolAirFlowRunner
    
## get datetime of now and the start of the day
start_date   = datetime.datetime.combine(datetime.date.today(), datetime.datetime.min.time())
airflow_date = Variable.get("today")
airflow_date = datetime.datetime.strptime(airflow_date, '%Y-%m-%d')
## if it new day bring the start of yesterday and start of today 
if start_date == airflow_date:
    end_date = start_date + timedelta(days=1)
else:
    end_date = start_date
    start_date = start_date - timedelta(days=1)

## get list of sites/ components
try:
    list_sites_meteocontrol = Variable.get("list_sites_meteocontrol", deserialize_json=True)
except:
    list_sites_meteocontrol = []

## brings the list of sites
def update_site_list(datum_freq_min):
    base_api = Meteocontrol('743db7bfb9e21c88ac001b742e84b64361fe1a66ba06f84a75885a3a5bff1deb', 'https://api.meteocontrol.de/v2/systems', 'Basic WW90YW1fa3N0OmthY28xMjM0NTY=')
    airflow_runner = MeteocontrolAirFlowRunner(base_api,"bse-bronze")
    # get site list
    sites = airflow_runner.get_sites_ids()
    #update airflow variable
    print(json.dumps(sites))
    Variable.set("list_sites_meteocontrol", json.dumps(sites))
    # sleep(seconds=10)
    print("Thanks you for getting sites list!")

## brings the list of inverters
def update_inverters_list(datum_freq_min, site_id):
    base_api = Meteocontrol('743db7bfb9e21c88ac001b742e84b64361fe1a66ba06f84a75885a3a5bff1deb', 'https://api.meteocontrol.de/v2/systems', 'Basic WW90YW1fa3N0OmthY28xMjM0NTY=')
    airflow_runner = MeteocontrolAirFlowRunner(base_api,"bse-bronze")
    # get inverter list
    inventer_id_list = []
    inventory = airflow_runner.get_sites_meta(str(site_id))
    inverters = json.loads(inventory.text)['data']
    for inverter in inverters:
        #single inverter
        print (inverter.get('id'))
        inventer_id_list.append(inverter.get('id'))
    Variable.set("site_"+str(site_id)+"_inverters_list",json.dumps(inventer_id_list))
    print("Thanks you for getting inverts list!")


def get_inverters_meta(datum_freq_min,site_id, inverter):
    base_api = Meteocontrol('743db7bfb9e21c88ac001b742e84b64361fe1a66ba06f84a75885a3a5bff1deb', 'https://api.meteocontrol.de/v2/systems', 'Basic WW90YW1fa3N0OmthY28xMjM0NTY=')
    airflow_runner = MeteocontrolAirFlowRunner(base_api,"bse-bronze")
    inverters_meta = airflow_runner.get_inverters_meta(str(site_id), inverter)
    print("inverters_meta:")
    print(inverters_meta)
    print("Thanks you for getting inverts list!")

## Brings the data by sites and by inverters
def update_inverters_data(datum_freq_min,site_id, date):
    base_api = Meteocontrol('743db7bfb9e21c88ac001b742e84b64361fe1a66ba06f84a75885a3a5bff1deb', 'https://api.meteocontrol.de/v2/systems', 'Basic WW90YW1fa3N0OmthY28xMjM0NTY=')
    airflow_runner = MeteocontrolAirFlowRunner(base_api,"bse-bronze")
    invereter_data = airflow_runner.get_invereter_data(str(site_id), date)
    print("invereter_data:")
    print(invereter_data)
    print("Thanks you for getting inverts list!")

with DAG(
    dag_id='get_all_sites_data_daily_meteocontrol',
    schedule_interval=None,
    start_date=pendulum.datetime(2022, 1, 1, tz="UTC"),
    catchup=False,
    concurrency=5,
    max_active_runs=1
) as dag:

    #dummy operators
    start = DummyOperator(task_id='start', dag=dag)
    start_inverters_list_meta_loop = DummyOperator(task_id='start_inverters_list_meta_loop', dag=dag)
    # start_inverters_meta_loop = DummyOperator(task_id='start_inverters_meta_loop', dag=dag)
    start_inverters_data_loop = DummyOperator(task_id='start_inverters_data_loop', dag=dag)
    end = DummyOperator(task_id='end', dag=dag, trigger_rule = TriggerRule.ONE_SUCCESS)

    update_site_list_task = PythonOperator(
        task_id='update_site_list_task',
        provide_context=True,
        dag=dag,
        op_kwargs={
                    "datum_freq_min": 15
                  },
        execution_timeout=timedelta(seconds=600),
        retries=2,
        retry_delay=timedelta(seconds=10),
        python_callable=update_site_list, 
        trigger_rule=TriggerRule.ALL_DONE)

    start >> update_site_list_task >> start_inverters_list_meta_loop

    num_rows = 100
    dummy_count = 1
    rows_count = 0
    dummy_prev_site = DummyOperator(task_id=f'dummy_site_{dummy_count - 1}', trigger_rule=TriggerRule.ALL_DONE, dag=dag)
    dummy_after_site = DummyOperator(task_id=f'dummy_site_{dummy_count}', trigger_rule=TriggerRule.ALL_DONE, dag=dag)

    start_inverters_list_meta_loop >> dummy_prev_site

    for site_id in list_sites_meteocontrol:

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

        update_inverters_data_task = PythonOperator(
            task_id = 'update_inverters_data_'+str(site_id)+'_'+str(start_date.date()),
            provide_context = True,
            dag = dag,
            op_kwargs = {
                            "datum_freq_min": 15,
                            "site_id": site_id,
                            "date": start_date.date()
                        },
            execution_timeout=timedelta(seconds=600),
            retries=2,
            retry_delay=timedelta(seconds=10),
            python_callable = update_inverters_data, 
            trigger_rule = TriggerRule.ALL_DONE)

        if rows_count == num_rows:
            rows_count = 0
            dummy_count += 1
            dummy_prev_site = dummy_after_site
            dummy_after_site = DummyOperator(task_id=f'dummy_site_{dummy_count}', trigger_rule=TriggerRule.ALL_DONE, dag=dag)    
        dummy_prev_site >> update_inverters_list_task >> update_inverters_data_task >> dummy_after_site
        rows_count = rows_count + 1

    num_rows = 100
    dummy_count = 1
    rows_count = 0
    dummy_prev_inverters = DummyOperator(task_id=f'dummy_inverters_{dummy_count - 1}', trigger_rule=TriggerRule.ALL_DONE, dag=dag)
    dummy_after_inverters = DummyOperator(task_id=f'dummy_inverters_{dummy_count}', trigger_rule=TriggerRule.ALL_DONE, dag=dag)

    dummy_after_site >> start_inverters_data_loop >> dummy_prev_inverters

    for site_id in list_sites_meteocontrol:

        try:
            list_inverters = Variable.get("site_"+str(site_id)+"_inverters_list", deserialize_json=True)
        except:
            list_inverters = []

        for inverter_id in list_inverters:
            
            update_inverters_meta_task = PythonOperator(
                task_id = 'update_inverters_meta_'+str(site_id)+'_'+str(inverter_id),
                provide_context = True,
                dag = dag,
                op_kwargs = {
                            "datum_freq_min": 15,
                            "site_id": site_id,
                            "serial_num": inverter_id
                            },
                execution_timeout=timedelta(seconds=600),
                retries=2,
                retry_delay=timedelta(seconds=10),
                python_callable = get_inverters_meta, 
                trigger_rule = TriggerRule.ALL_DONE)

            if rows_count == num_rows:
                rows_count = 0
                dummy_count += 1
                dummy_prev_inverters = dummy_after_inverters
                dummy_after_inverters = DummyOperator(task_id=f'dummy_inverters_{dummy_count}', trigger_rule=TriggerRule.ALL_DONE, dag=dag)    
            dummy_prev_inverters >> update_inverters_meta_task >> dummy_after_inverters
            rows_count = rows_count + 1
    dummy_after_inverters >> end

# update_site_list(15)
# for site_id in list_sites_meteocontrol:
#     update_inverters_list(15, site_id)

#     try:
#         list_inverters = Variable.get("site_"+str(site_id)+"_inverters_list", deserialize_json=True)
#     except:
#         list_inverters = []

#     for inverter_id in list_inverters:
#         get_inverters_meta(15, site_id, inverter_id)
    
#     update_inverters_data(15, site_id, start_date.date())