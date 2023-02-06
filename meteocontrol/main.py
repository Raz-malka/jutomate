import argparse
from datetime import datetime, timedelta
import logging
import sys
from time import sleep
from Meteocontrol import Meteocontrol
from filesutil import Files
import json
import os
import requests
from trackingutils import PositionControl
from datesutil import Dates
logger  = logging.getLogger()



def main(api_key, base_url, authorization, files_destination, get_meta, sites_map_group):
    if not os.path.exists(files_destination):
        raise FileNotFoundError(f'file {files_destination} not found!')
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    meteocontrol = Meteocontrol(api_key, base_url, authorization)

    if get_meta:
        try:
            sites_map = meteocontrol.get_sites()
        except requests.exceptions.RequestException as e:
            meteocontrol.error_log(e.args[0].args[0])
            sys.exit()      
        except Exception as e:
            logger.exception("Error getiing sites!", e) 
            sys.exit()
        Files.make_file(sites_map, files_destination, file_name=sites_map_group)
        sites_map_data = sites_map.json()['data']
        sites = [key['key'] for key in sites_map_data]
    else:
        with open(os.path.join(files_destination, sites_map_group + '.json'), 'r') as file:
            sites_map_data = json.load(file)['data']
        sites = [key['key'] for key in sites_map_data]

    now = Dates.today
    PositionControl.set_boundry_for_data_formating(files_destination, now)
    for site in sites:
        if get_meta:
            try:
                site_details, site_technical_data, inverters_list, inverters_list_detailed = get_meta_data(meteocontrol, site)
            except requests.exceptions.RequestException as e:
                meteocontrol.error_log(e.args[0].args[0])
            except Exception as e:
                logger.exception(f"Error getiing site id: {site} meta data", e) 
            inverters = {}
            inverters['inverters'] = inverters_list_detailed
            Files.make_file(site_details, files_destination, 'sites', f"{site}", "meta_data", file_name="site_details")
            Files.make_file(site_technical_data, files_destination, 'sites', f"{site}", "meta_data", file_name="site_technical_data")
            Files.make_file(inverters_list, files_destination, 'sites', f"{site}", "meta_data", file_name="inverters")
            Files.make_file(inverters, files_destination, 'sites', f"{site}", "meta_data", file_name="inverters_detailed")

        site_saved, start_date = PositionControl.get_last_request_status(site, files_destination)
        if not site_saved:
            PositionControl.set_last_request_status_id(site, files_destination)
        if start_date is None:
            start_date = datetime.strptime("2023-01-01", '%Y-%m-%d')
        end_date = datetime.now()
        curr_date = start_date.date()
        while curr_date <= end_date.date():
            try:
                site_data = meteocontrol.get_site_data(site, curr_date)
            except requests.exceptions.RequestException as e:
                meteocontrol.error_log(e.args[0].args[0])
                break # move to next site
            except Exception as e:
                logger.exception(f"Error getiing site id: {site} site data", e)
                break # move to next site
            # possibility to add curr_day to make year, month, day files for uploading to DB
            Files.make_file(site_data.json(), files_destination, 'sites', f"{site}", "data", curr_date.year, curr_date.month, curr_date.day, file_name=str(curr_date))
            PositionControl.set_last_request_status_date(site, curr_date, files_destination)
            curr_date = curr_date + timedelta(days=1)
            sleep(1)


def get_meta_data(meteocontrol: Meteocontrol, site):
    try:
        site_technical_data = meteocontrol.get_site_technical_data(site)
    except requests.exceptions.RequestException as e:
            raise requests.exceptions.RequestException(e.args[0])
    except Exception as e:
            raise Exception(e)
    sleep(1)

    try:
        site_details = meteocontrol.get_site_details(site)
    except requests.exceptions.RequestException as e:
            raise requests.exceptions.RequestException(e.args[0])
    except Exception as e:
            raise Exception(e)
    sleep(1)

    try:
        inverters = meteocontrol.get_inverters(site)
    except requests.exceptions.RequestException as e:
            raise requests.exceptions.RequestException(e.args[0])
    except Exception as e:
            raise Exception(e)
    sleep(1)

    inverters_details = []
    for inverter in inverters.json()['data']:
        try:
            inverter_details = meteocontrol.get_inverter_details(site, inverter['id'])
            inverters_details.append(inverter_details.json()['data'])
        except Exception as e:
            logger.error(msg=f'Can not get data for {inverter} in site {site}!\nmsg: {e}')
        sleep(1)

    return site_details, site_technical_data, inverters, inverters_details




if __name__ == "__main__":

    # parser obj
    parser = argparse.ArgumentParser()
    parser.add_argument('-k', '--key', metavar='', type=str, help='Enter your api key', default='8ddf605143f791ddd956d7f8f3bd48ae8ff45f5754d09804fc2ad3a8690e3960')
    parser.add_argument('-fmd', '--files_destination', metavar='', type=str, help='Enter files destination', default=r'\\filer2\DataLake\S3\bronze\meteocontrol')
    parser.add_argument('-auth', metavar='', type=str, help='Enter authorization', default='Basic WW90YW1fa3N0OmthY28xMjM0NTY=')
    parser.add_argument('-url', metavar='', type=str, help='Enter main url', default='https://api.meteocontrol.de/v2/systems')
    parser.add_argument('-m', metavar='', type=bool, help='Enter True if you want to update meta data', default=False)
    args = parser.parse_args()

    # global parm
    api_key = args.key
    files_destination = args.files_destination
    authorization = args.auth
    base_url = args.url
    get_meta = args.m
    if authorization == 'Basic WW90YW1fa3N0OmthY28xMjM0NTY=':
        sites_map_group = 'site_map_stc'
    else:
        sites_map_group = 'site_map_profits'

    main(api_key, base_url, authorization, files_destination, get_meta, sites_map_group)