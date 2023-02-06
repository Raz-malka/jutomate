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
from trackingutils import PositionControl
from datesutil import Dates
from Meteocontrol import Meteocontrol
from meteocontrol_airflow_runner import meteocontrol_airflow_runner
    
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
list_sites_meteocontrol     = Variable.get("list_sites_meteocontrol", deserialize_json=True)

## brings the list of sites
def update_site_list(datum_freq_min):
    base_api = Meteocontrol('743db7bfb9e21c88ac001b742e84b64361fe1a66ba06f84a75885a3a5bff1deb', 'https://api.meteocontrol.de/v2/systems', 'Basic WW90YW1fa3N0OmthY28xMjM0NTY=')
    airflow_runner = meteocontrol_airflow_runner(base_api,"bse-bronze")
    # get site list
    sites = airflow_runner.get_sites()
    site_id_list = []
    for site_dict in sites['data']:
        site_id_list.append(site_dict.get('key'))
    #update airflow variable
    print(json.dumps(site_id_list))
    Variable.set("list_sites_meteocontrol", json.dumps(site_id_list))
    print("Thanks you for getting sites list!")

## brings the list of inverters
def update_inverters_list(datum_freq_min, site_id):
    base_api = Meteocontrol('743db7bfb9e21c88ac001b742e84b64361fe1a66ba06f84a75885a3a5bff1deb', 'https://api.meteocontrol.de/v2/systems', 'Basic WW90YW1fa3N0OmthY28xMjM0NTY=')
    airflow_runner = meteocontrol_airflow_runner(base_api,"bse-bronze")
    # get inverter list
    inventer_id_list = []
    inventory = airflow_runner.get_inverters(str(site_id))
    inverters = json.loads(inventory.text)['data']
    for inverter in inverters:
        #single inverter
        print (inverter.get('id'))
        inventer_id_list.append(inverter.get('id'))
    Variable.set("site_"+str(site_id)+"_inverters_list",json.dumps(inventer_id_list))
    print("Thanks you for getting inverts list!")

## Brings the data by sites and by inverters
def update_inverters_data(datum_freq_min,site_id, serial_num, start_date, end_date):
    base_api = Meteocontrol('743db7bfb9e21c88ac001b742e84b64361fe1a66ba06f84a75885a3a5bff1deb', 'https://api.meteocontrol.de/v2/systems', 'Basic WW90YW1fa3N0OmthY28xMjM0NTY=')
    airflow_runner = meteocontrol_airflow_runner(base_api,"bse-bronze")
    invereter_data = airflow_runner.get_invereter_data(str(site_id), serial_num, start_date, end_date)
    print("invereter_data:")
    print(invereter_data)
    print("Thanks you for getting inverts list!")


update_site_list(15)
for site_id in list_sites_meteocontrol:
    update_inverters_list(15, site_id)

    try:
        list_inverters = Variable.get("site_"+str(site_id)+"_inverters_list", deserialize_json=True)
    except:
        list_inverters = []

    for inverter_id in list_inverters:
        update_inverters_data(15, site_id, inverter_id, start_date, end_date)