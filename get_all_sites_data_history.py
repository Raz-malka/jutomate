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
from dateutil.relativedelta import relativedelta
from airflow import DAG
from airflow import AirflowException
from SolarEdge import Solaredge
from SolarEdgeAirFlowRunner import SolaredgeAirFlowRunner


## get datetime from variable history_loop_start_date (part of hisroty)
start_date = Variable.get("history_loop_start_date")
end_date   = Variable.get("history_loop_end_date")
if (start_date is not None and  end_date is not None):
	start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d').date()
	end_date   = datetime.datetime.strptime(end_date, '%Y-%m-%d').date()

## get list of sites/ components
list_sites = Variable.get("list_sites", deserialize_json=True)

# Brings days one after the other according to the range of days we bring to him
def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + timedelta(n)

# Updates days in the variable loop_start_date and loop_end_date that day and the day after (for pulling the information according to specific days)
def datelist(single_date):
    Variable.set("loop_start_date", single_date)
    Variable.set("loop_end_date", single_date + relativedelta(days=1))
    time.sleep(10)

# Pulls from api a list of sites
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

# Pulls from api a list of inverters
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

with DAG(
    dag_id='get_all_sites_data_history',
    schedule_interval=None,
    start_date=pendulum.datetime(2022, 1, 1, tz="UTC"),
    catchup=False,
    concurrency=10,
    max_active_runs=1
) as dag:

    #dummy operators
    start = DummyOperator(task_id='start', dag=dag)
    start_site_inverters_loop = DummyOperator(task_id='start_site_inverters_loop', dag=dag)
    start_site_inverters_date = DummyOperator(task_id='start_site_inverters_date', dag=dag)
    end = DummyOperator(task_id='end', dag=dag, trigger_rule = TriggerRule.ALL_DONE)

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

    num_rows = 50
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
            python_callable = update_inverters_list, 
            trigger_rule = TriggerRule.ALL_DONE)
        if rows_count == num_rows:
            rows_count = 0
            dummy_count += 1
            dummy_prev_site = dummy_after_site
            dummy_after_site = DummyOperator(task_id=f'dummy_site_{dummy_count}', trigger_rule=TriggerRule.ALL_DONE, dag=dag)    
        dummy_prev_site >> update_inverters_list_task >> dummy_after_site
        rows_count = rows_count + 1

    num_rows = 1
    dummy_count = 1
    rows_count = 0
    dummy_prev_date = DummyOperator(task_id=f'dummy_date_{dummy_count - 1}', trigger_rule=TriggerRule.ALL_DONE, dag=dag)
    dummy_after_date = DummyOperator(task_id=f'dummy_date_{dummy_count}', trigger_rule=TriggerRule.ALL_DONE, dag=dag)

    dummy_after_site >> start_site_inverters_date >> dummy_prev_date

    for single_date in daterange(start_date, end_date):

        ## loop updates days in the variable loop_start_date and loop_end_date that day and the day after
        update_inverters_list_task = PythonOperator(
            task_id = 'date_list_'+str(single_date),
            provide_context = True,
            dag = dag,
            op_kwargs = {
                            "single_date": single_date
                        },
            python_callable = datelist, 
            trigger_rule = TriggerRule.ALL_DONE)
        
        # Trigger that turns on the dag: 'get_all_data_history' that brings the data according to the variable loop_start_date and loop_end_date 
        run_date_range = TriggerDagRunOperator(
            task_id=f'run_date_{str(single_date)}',
            trigger_dag_id='get_all_data_history',
            wait_for_completion=True,trigger_rule=TriggerRule.ALL_DONE,
            dag=dag
        )
        if rows_count == num_rows:
            rows_count = 0
            dummy_count += 1
            dummy_prev_date = dummy_after_date
            dummy_after_date = DummyOperator(task_id=f'dummy_date_{dummy_count}', trigger_rule=TriggerRule.ALL_DONE, dag=dag)    
        dummy_prev_date >> update_inverters_list_task >> run_date_range >> dummy_after_date
        rows_count = rows_count + 1

    dummy_after_date >> end
