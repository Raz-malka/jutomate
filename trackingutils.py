import datetime
import pandas as pd
import numpy as np
from os.path import exists
from utils.datesutil import Dates



class PositionControl:

    @classmethod
    def get_last_request_status(cls, index: int, path: str) -> int and datetime and str:
        if exists(f'{path}\last_request_status.csv'):
            df = pd.read_csv(f'{path}\last_request_status.csv')
            df = df.replace({np.nan: None})
            if index < len(df['site_id']):
                request_site_id = int(df.loc[index, 'site_id'])
                last_date = df.loc[index, 'last_date']
                serial_number = df.loc[index, 'serial_number']
                if last_date:
                    last_date = Dates.get_datetime_obj(last_date)
                return request_site_id, last_date, serial_number
        return None, None, None


    @classmethod
    def set_last_request_status_id(cls, index: int, site_id: int or str, path: str):
        if not exists(f'{path}\last_request_status.csv'):
            df = pd.DataFrame(columns=['site_id', 'serial_number', 'last_date'])
            df.to_csv(f'{path}\last_request_status.csv', index=False)
            
        df = pd.read_csv(f'{path}\last_request_status.csv')
        df.loc[index, 'site_id'] = str(site_id)
        df.to_csv(f'{path}\last_request_status.csv', index=False)


    @classmethod
    def set_last_request_status_date(cls, index: int, last_date: datetime or str, path: str):
        df = pd.read_csv(f'{path}\last_request_status.csv')
        df.loc[index, 'last_date'] = last_date 
        df.to_csv(f'{path}\last_request_status.csv', index=False)


    @classmethod
    def set_last_request_status_serial_number(cls, index: int , site_id: int or str, last_date: datetime or str, serial_number: str, path: str):
        df = pd.read_csv(f'{path}\last_request_status.csv')
        df.loc[index] = str(site_id), serial_number, last_date 
        df.to_csv(f'{path}\last_request_status.csv', index=False)