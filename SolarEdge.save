from ast import Tuple
from Api import Api
from datetime import datetime
from const import PAGE_SIZE
from utils.datesutil import Dates



class Solaredge(Api):

    '''
    this class enables getting data from solaredge api data.
    IMPORTANT! functions will return the json format data in a dict, but if the request goes wrong, it will return the response status as an int!

    ***** POSSIBLE IMPROVEMENT - ASYNC *****
    '''

    def __init__(self, api_key: str, base_url: str) -> None:
        super().__init__(api_key, base_url)


    # 'sites_details' returns same as 'sites' but has also the count of all sites
    def get_sites(self): 
        index = 0
        count_sites = 1
        sites_details = {}

        while index < count_sites:
            url = f'{self.base_url}/sites/list.json?startIndex={index}&api_key={self.api_key}'
            data = self.get_json_data(url)
            if isinstance(data, int):
                return data
            if index == 0:
                sites_details.update(data)
                count_sites = sites_details['sites']['count']
            else:
                for site in data['sites']['site']:
                    sites_details['sites']['site'].append(site)
            index += PAGE_SIZE

        sites = sites_details['sites']['site']
        return sites_details, sites


    def get_accounts(self) -> dict or int:
        url = f'{self.base_url}/accounts/list.json?api_key={self.api_key}'
        accounts = self.get_json_data(url)
        return accounts


    # input: dict of site details
    get_site_name = lambda self, site : site['name'] # returns str
    get_site_id = lambda self, site : site['id'] # returns int


    def get_overview(self, site_id: int or str) -> dict or int:
        url = f'{self.base_url}/site/{site_id}/overview.json?api_key={self.api_key}'
        overview = self.get_json_data(url)
        return overview

    
    def get_current_power_flow(self, site_id: int or str) -> dict or int:
        url = f'{self.base_url}/site/{site_id}/currentPowerFlow.json?api_key={self.api_key}'
        current_power_flow = self.get_json_data(url)
        return current_power_flow


    def get_environmental_benefits(self, site_id: int or str) -> dict or int:
        url = f'{self.base_url}/site/{site_id}/envBenefits.json?systemUnits=Metrics&api_key={self.api_key}'
        environmental_benefits = self.get_json_data(url)
        return environmental_benefits


    def get_inventory(self, site_id: int or str) -> dict or int:
        url = f'{self.base_url}/site/{site_id}/inventory.json?api_key={self.api_key}'
        inventory = self.get_json_data(url)
        return inventory


    def get_sensors(self, site_id: int or str) -> dict or int:
        url = f'{self.base_url}/equipment/{site_id}/sensors.json?api_key={self.api_key}'
        sensors = self.get_json_data(url)
        return sensors


    def get_change_log(self, site_id: int or str, serial_number: str) -> dict or int:
        url = f'{self.base_url}/equipment/{site_id}/{serial_number}/changeLog.json?api_key={self.api_key}'
        change_log = self.get_json_data(url)
        return change_log


    def get_data_period(self, site_id: int or str) -> "tuple[dict or int, datetime, datetime]" or int:
        url = f'{self.base_url}/site/{site_id}/dataPeriod.json?api_key={self.api_key}'
        data_period = self.get_json_data(url)
        if isinstance(data_period, int):
            return data_period, None, None
        if data_period['dataPeriod']['startDate']:
            start_date = datetime.strptime(data_period['dataPeriod']['startDate'], '%Y-%m-%d')
            end_date = datetime.strptime(data_period['dataPeriod']['endDate'], '%Y-%m-%d')
        else:
            start_date = None
            end_date = None

        return data_period, start_date, end_date

    # 'components_details' returns same as 'components' but has also the count of all components in site
    def get_components(self, site_id: int or str) -> "tuple[dict, list]" or int:
        url = f'{self.base_url}/equipment/{site_id}/list.json?api_key={self.api_key}'
        components_details = self.get_json_data(url)
        # if response is not the propper data return the response status
        if isinstance(components_details, int):
            return components_details, None
        components = components_details['reporters']['list']

        return components_details, components


    # input: dict of component
    get_serial_number = lambda self, component : component['serialNumber']


    def get_energy_measurements(self, site_id: int or str, start_date: datetime, end_date: datetime, time_unit: str='QUARTER_OF_AN_HOUR') -> dict or int:
        if start_date:
            url = f'{self.base_url}/site/{site_id}/energy.json?timeUnit={time_unit}&endDate={end_date.date()}&startDate={start_date.date()}&api_key={self.api_key}'
            energy_measurements = self.get_json_data(url)
            return energy_measurements


    def get_power_measurements(self, site_id: int or str, start_date: datetime, end_date: datetime, time_unit: str='QUARTER_OF_AN_HOUR') -> dict or int:
        if start_date:
            if start_date == end_date: # if want to mesure one full day         # CHECK IF WORCKS WITH DATETIME OBJ
                start_date = Dates.set_time_to_begining_of_date(start_date)
                end_date = Dates.set_time_to_end_of_date(end_date)
            url = f'{self.base_url}/site/{site_id}/power.json?timeUnit={time_unit}&startTime={start_date}&endTime={end_date}&api_key={self.api_key}'
            power_measurements = self.get_json_data(url)
            return power_measurements


    def get_power_details(self, site_id: int or str, start_date: datetime, end_date: datetime, time_unit: str='QUARTER_OF_AN_HOUR') -> dict or int:
        if start_date:
            if start_date == end_date: # if want to mesure one full day         
                start_date = Dates.set_time_to_begining_of_date(start_date)
                end_date = Dates.set_time_to_end_of_date(end_date)
            url = f'{self.base_url}/site/{site_id}/powerDetails.json?timeUnit={time_unit}&startTime={start_date}&endTime={end_date}&api_key={self.api_key}'
            power_details = self.get_json_data(url)
            return power_details


    def get_energy_details(self, site_id: int or str, start_date: datetime, end_date: datetime, time_unit: str='QUARTER_OF_AN_HOUR') -> dict or int:
        if start_date:
            if start_date == end_date: # if want to mesure one full day         
                start_date = Dates.set_time_to_begining_of_date(start_date)
                end_date = Dates.set_time_to_end_of_date(end_date)
            url = f'{self.base_url}/site/{site_id}/energyDetails.json?timeUnit={time_unit}&startTime={start_date}&endTime={end_date}&api_key={self.api_key}'
            energy_details = self.get_json_data(url)
            return energy_details
    

    def get_meters(self, site_id: int or str, start_date: datetime, end_date: datetime, time_unit: str='QUARTER_OF_AN_HOUR') -> dict or int:
        if start_date:
            if start_date == end_date: # if want to mesure one full day         
                start_date = Dates.set_time_to_begining_of_date(start_date)
                end_date = Dates.set_time_to_end_of_date(end_date)
            url = f'{self.base_url}/site/{site_id}/meters.json?timeUnit={time_unit}&startTime={start_date}&endTime={end_date}&api_key={self.api_key}'
            meters = self.get_json_data(url)
            return meters


    def get_storage_data(self, site_id: int or str, serial_num: str, start_date: datetime, end_date: datetime) -> dict or int:
        if start_date:
            if start_date == end_date: # if want to mesure one full day         
                start_date = Dates.set_time_to_begining_of_date(start_date)
                end_date = Dates.set_time_to_end_of_date(end_date)
            url = f'{self.base_url}/site/{site_id}/storageData.json?serials={serial_num}&startTime={start_date}&endTime={end_date}&api_key={self.api_key}'
            storage_data = self.get_json_data(url)
            return storage_data


    def get_invereter_data(self, site_id: int or str, serial_num: str, start_date: datetime, end_date: datetime) -> dict or int:
        if start_date:
            if start_date == end_date: # if want to mesure one full day         
                start_date = Dates.set_time_to_begining_of_date(start_date)
                end_date = Dates.set_time_to_end_of_date(end_date)
            url = f'{self.base_url}/equipment/{site_id}/{serial_num}/data.json?startTime={start_date}&endTime={end_date}&api_key={self.api_key}'
            invereter_data = self.get_json_data(url)
            return invereter_data


    def get_sensors_data(self, site_id: int or str, start_date: datetime, end_date: datetime) -> dict or int:
        if start_date:
            if start_date == end_date: # if want to mesure one full day         
                start_date = Dates.set_time_to_begining_of_date(start_date)
                end_date = Dates.set_time_to_end_of_date(end_date)
            url = f'{self.base_url}/site/{site_id}/sensors.json?startDate={start_date}&endDate={end_date}&api_key={self.api_key}'
            sensors_data = self.get_json_data(url)
            return sensors_data