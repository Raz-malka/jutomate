import logging
import os
import json
import pandas as pd


class Files:

    @classmethod
    def get_new_path(cls, *paths: str or int) -> str:
        str_path = [str(path) for path in paths]
        new_path = os.path.join(*str_path)
        return new_path


    @classmethod
    def make_file(cls, data, *paths: str or int, file_name: str, file_type: str='.json'):
        # if file exisits it will update file
        # make folders
        if not isinstance(data, dict):
            data = data.json()
        new_path = cls.get_new_path(*paths)
        full_path = os.path.join(new_path, file_name)
        if os.path.exists(new_path):
            if os.path.exists(full_path):
                # get data on file
                with open(full_path + file_type, 'r', encoding='utf-8') as file:
                    file_data = json.load(file)
                    file_data.update(data)
                    data = file_data
        else:
            os.makedirs(new_path)

        # make new/updated data file
        with open(full_path + file_type, 'w', encoding='utf-8') as file:
            json.dump(data, file, indent=4, ensure_ascii=False)
            

    @classmethod
    def get_overview_df(cls, destination, file_name):
        file = os.path.join(destination, file_name)
        if os.path.exists(file):
            with open(file, 'r', encoding='utf-8') as file_data:
                data = json.load(file_data)
                lastUpdateTime = data['overview']['lastUpdateTime']
                df_lifeTimeEnergy = pd.DataFrame({'Id':[1], 'UpdateTimeUtc':[lastUpdateTime], 'Value':[data['overview']['lifeTimeData']['energy']]})
                df_lastYearEnergy = pd.DataFrame({'Id':[2], 'UpdateTimeUtc':[lastUpdateTime], 'Value':[data['overview']['lastYearData']['energy']]})
                df_lastMonthEnergy = pd.DataFrame({'Id':[3], 'UpdateTimeUtc':[lastUpdateTime], 'Value':[data['overview']['lastMonthData']['energy']]})
                df_lastDayEnergy = pd.DataFrame({'Id':[4], 'UpdateTimeUtc':[lastUpdateTime], 'Value':[data['overview']['lastDayData']['energy']]})
                return df_lifeTimeEnergy, df_lastYearEnergy, df_lastMonthEnergy, df_lastDayEnergy
        logging.exception(msg=f'File {file} not found!')
        return None, None, None, None

    @classmethod
    def get_energy_df(cls, destination, year, month, day, file_name):
        file = os.path.join(destination, str(year), str(month), str(day), file_name)
        df = pd.DataFrame()
        if os.path.exists(file):
            with open(file, 'r', encoding='utf-8') as file_data:
                data = json.load(file_data)
                for time in data['energy']['values']:
                    df = pd.concat([df, pd.DataFrame({'Id':[6], 'UpdateTimeUtc':[time['date']], 'Value':[time['value']]})], axis=0)
                return df
        logging.exception(msg=f'File {file} not found!')


    @classmethod
    def get_power_df(cls, destination, year, month, day, file_name):
        file = os.path.join(destination, str(year), str(month), str(day), file_name)
        df = pd.DataFrame()
        if os.path.exists(file):
            with open(file, 'r', encoding='utf-8') as file_data:
                data = json.load(file_data)
                for time in data['power']['values']:
                    df = pd.concat([df, pd.DataFrame({'Id':[5], 'UpdateTimeUtc':[time['date']], 'Value':[time['value']]})], axis=0)
                return df
        logging.exception(msg=f'File {file} not found!')
            