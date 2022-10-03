import json
from datetime import datetime
from SolarEdge import Solaredge
import boto3

BUCKET_NAME_KEY = '<bucket>'
DATE_KEY = '<date>'
SITE_ID_KEY = '<site_id>'
OBJECT_ID_KEY = '<object_id>'
TIME_KEY = '<time>'
SITES_METADATA_PATH_PATTERN = f'sites_metadata/dt={DATE_KEY}/site_details.json'
SITE_INVENTORY_PATH_PATTERN = f'sites_invertory_details/dt={DATE_KEY}/site_id={SITE_ID_KEY}/{BUCKET_NAME_KEY}_sites_invertory_details_{DATE_KEY}_site_id_{SITE_ID_KEY}.json'
INVERTER_PATH_PATTERN = f'inverters_data/dt={DATE_KEY}/site_id={SITE_ID_KEY}/inv_id={OBJECT_ID_KEY}/{BUCKET_NAME_KEY}_inverters_data_{DATE_KEY}_site_id_{SITE_ID_KEY}_inv_id_{OBJECT_ID_KEY}_{TIME_KEY}.json'

class SolaredgeAirFlowRunner:


    def __init__(self, base_api: Solaredge, bucket_name: str) -> None:
        self.base_api = base_api
        self.bucket_name = bucket_name

    def write_to_s3(self, json_data, path_pattern: str, start_time: datetime, site_id = None, object_id: str = None ):
        s3 = boto3.resource('s3')
        print(start_time)
        date = start_time.strftime('%Y-%m-%d')
        time = start_time.strftime("%H:%M:%S")
        path = path_pattern.replace(BUCKET_NAME_KEY, self.bucket_name)
        if date is not None:
            path = path.replace(DATE_KEY, date)
        if time is not None:
            path = path.replace(TIME_KEY, time)
        if site_id is not None:
            path = path.replace(SITE_ID_KEY, site_id)
        if object_id is not None:
            path = path.replace(OBJECT_ID_KEY, object_id)

        s3.Bucket(self.bucket_name).put_object(Key= path, Body=(bytes(json.dumps(json_data).encode('UTF-8'))))

    def get_sites(self): 
        sites_details = self.base_api.get_sites()
        sites_details = sites_details[0]
        self.write_to_s3(sites_details, path_pattern = SITES_METADATA_PATH_PATTERN, start_time = datetime.today())
        return sites_details

    def get_inventory(self, site_id: int or str) -> dict or int:
        site_inventory = self.base_api.get_inventory(site_id)
        self.write_to_s3(site_inventory, path_pattern = SITE_INVENTORY_PATH_PATTERN, start_time =  datetime.today(), site_id = site_id)
        return site_inventory

    def get_invereter_data(self, site_id: int or str, serial_num: str, start_date: datetime, end_date: datetime) -> dict or int:
        inverter_data = self.base_api.get_invereter_data(site_id, serial_num, start_date, end_date)
        self.write_to_s3(inverter_data, INVERTER_PATH_PATTERN, site_id = site_id, start_time= start_date, object_id= serial_num)
        return inverter_data