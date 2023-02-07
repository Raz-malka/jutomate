from datetime import datetime
import logging
import pandas as pd
import numpy as np
from os.path import exists
from meteocontrol.datesutil import Dates



class PositionControl:

    @classmethod
    def get_last_request_status(cls, site_id: int, path: str) -> bool and datetime:
        site_visited = False
        if exists(f'{path}\last_request_status.csv'):
            df = pd.read_csv(f'{path}\last_request_status.csv', index_col='site_id')

            if site_id in df.index:
                site_visited = True
                df = df.replace({np.nan: None})
                #df.sort_index(inplace=True, level=0)
                last_date = df.loc[site_id, 'last_date']
                if last_date:
                    last_date = datetime.strptime(last_date, '%Y-%m-%d')
                    logging.info(msg=f'{site_visited}, {last_date}')
                return site_visited, last_date
        return site_visited, None


    @classmethod
    def set_last_request_status_id(cls, site_id: int, path: str):
        if not exists(f'{path}\last_request_status.csv'):
            df = pd.DataFrame(columns=['site_id', 'last_date'])
            df.to_csv(f'{path}\last_request_status.csv', index=False)

            
        df = pd.read_csv(f'{path}\last_request_status.csv', index_col='site_id')
        #df.set_index('serial_number', inplace=True, append=True)
        df.loc[site_id] = ""
        df.to_csv(f'{path}\last_request_status.csv')


    @classmethod
    def set_boundry_for_data_formating(cls, path: str, now: datetime):
        df = pd.read_csv(f'{path}\last_request_status.csv', index_col='site_id')
        df.loc['now', 'last_date'] = now
        df.to_csv(f'{path}\last_request_status.csv')


    @classmethod
    def set_last_request_status_date(cls, site_id: int, last_date: datetime or str, path: str):
        df = pd.read_csv(f'{path}\last_request_status.csv', index_col='site_id')
        df.loc[site_id, 'last_date'] = last_date
        df.to_csv(f'{path}\last_request_status.csv')