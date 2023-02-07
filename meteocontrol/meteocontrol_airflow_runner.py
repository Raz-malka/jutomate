import json
from datetime import datetime
from meteocontrol.Meteocontrol import Meteocontrol
import boto3

BUCKET_NAME_KEY = '<bucket>'
DATE_KEY = '<date>'
SITE_ID_KEY = '<site_id>'
OBJECT_ID_KEY = '<object_id>'
TIME_KEY = '<time>'
SITES_NAMES_MAP_PATH_PATTERN = f'sites_metadata_meteocontrol/dt={DATE_KEY}/sites_names_map.json'
SITES_TECHNICAL_DATA_PATH_PATTERN = f'sites_metadata_meteocontrol/dt={DATE_KEY}/site_technical_data.json'
SITES_DETAILS_PATH_PATTERN = f'sites_metadata_meteocontrol/dt={DATE_KEY}/site_details.json'
SITES_INVERTERS_PATH_PATTERN = f'sites_metadata_meteocontrol/dt={DATE_KEY}/inverters.json'
SITE_INVENTORY_PATH_PATTERN = f'sites_invertory_details_meteocontrol/dt={DATE_KEY}/site_id={SITE_ID_KEY}/{BUCKET_NAME_KEY}_sites_invertory_details_{DATE_KEY}_site_id_{SITE_ID_KEY}.json'
INVERTER_PATH_PATTERN = f'inverters_data_meteocontrol/dt={DATE_KEY}/site_id={SITE_ID_KEY}/inv_id={OBJECT_ID_KEY}/{BUCKET_NAME_KEY}_inverters_data_{DATE_KEY}_site_id_{SITE_ID_KEY}_inv_id_{OBJECT_ID_KEY}_{TIME_KEY}.json'

class MeteocontrolAirFlowRunner:


    def __init__(self, base_api: Meteocontrol, bucket_name: str) -> None:
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


    def get_sites_ids(self): 
        sites_names_map = self.base_api.get_sites()
        sites_names_map = sites_names_map.json()
        sites_ids = [key['key'] for key in sites_names_map['data']]
        self.write_to_s3(sites_names_map, path_pattern = SITES_NAMES_MAP_PATH_PATTERN, start_time = datetime.today())
        return sites_ids

    
    def get_sites_meta(self, site_id):
        # for site in sites:
        site_technical_data = self.base_api.get_site_technical_data(site_id)
        site_details = self.base_api.get_site_details(site_id)
        inverters = self.base_api.get_inverters(site_id)
        self.write_to_s3(site_technical_data.json(), path_pattern = SITES_TECHNICAL_DATA_PATH_PATTERN, start_time = datetime.today())
        self.write_to_s3(site_details.json(), path_pattern = SITES_DETAILS_PATH_PATTERN, start_time = datetime.today())
        self.write_to_s3(inverters.json(), path_pattern = SITES_INVERTERS_PATH_PATTERN, start_time = datetime.today())
        return inverters

    def get_inverters_meta(self, site_id, inverter):
        inverter_details = self.base_api.get_inverter_details(site_id, inverter)
        inverter_details = inverter_details.json()
        self.write_to_s3(inverter_details, path_pattern = SITE_INVENTORY_PATH_PATTERN, start_time =  datetime.today(), site_id = site_id)
        return inverter_details


    def get_invereter_data(self, site_id, date):
        site_data = self.base_api.get_site_data(site_id, date)
        site_data = site_data.json()
        self.write_to_s3(site_data, INVERTER_PATH_PATTERN, site_id = site_id, start_time= date)
        return site_data