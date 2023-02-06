from os.path import exists
import pandas as pd
import re
import numpy as np

class SavedDate:

    @classmethod
    def get_last_formated(cls, site: int or str, path: str, file_name="last_formated"):
        if exists(f'{path}\{file_name}.csv'):
            df = pd.read_csv(f'{path}\last_formated.csv', index_col='site')
            df.replace(np.nan, None, inplace=True)
            if site in df.index:
                date = df.loc[site, 'date']
                if not isinstance(date, pd.Series):
                    date_parts = date.split('-')
                    year = date_parts[0]
                    month = date_parts[1]
                    day = re.match(r'^\d{1,2}',date_parts[2]).group(0)
                    return int(year), int(month), int(day)
        return None, None, None


    @classmethod
    def save_formated_date(cls, site, date_time, path, file_name="last_formated"):
        if not exists(f'{path}\{file_name}.csv'):
            df = pd.DataFrame(columns=['site', 'date'])
            df.to_csv(f'{path}\{file_name}.csv', index=False)

        df = pd.read_csv(f'{path}\{file_name}.csv', index_col='site') 

        if site in df.index:
            df.at[site, 'date'] = date_time
        else:
            df.loc[site] = ''  
        
        df.to_csv(f'{path}\{file_name}.csv')   