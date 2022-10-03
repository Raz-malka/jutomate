'''
**still in development**
program get sites name, peak power and address from existing json file and outputs a csv file containing all input and adds latitude, longitude using google maps api.
the program will produce a log file. if there is an error with decoding it will show in the log file.
possibility making async.


EXAMPLE OF INPUT
{
  "sites": {
    "count": 143,
    "site": [
      {
        "id": 2860710,
        "name": " הגבעה סככה מזרחית , אשקלון",
        "accountId": 138818,
        "status": "PendingCommunication",
        "peakPower": 300.0,
        "installationDate": "2022-04-26",
        "ptoDate": null,
        "notes": "",
        "type": "Optimizers & Inverters",
        "location": {
          "country": "Israel",
          "city": "Ashkelon",
          "address": "Ashkelon",
          "address2": "הגבעה אשקלון ",
          "zip": "000000000",
          "timeZone": "Asia/Jerusalem",
          "countryCode": "IL"
        },
        "publicSettings": {
          "isPublic": false
        }
      }
    }]
}
'''

import json
from geopy.geocoders import GoogleV3
import csv
import logging
import time


curr_time = time.strftime("%d-%m-%Y_%H-%M")
logging.basicConfig(level=logging.INFO, filename=f'geocode_{curr_time}.log', filemode='w', format='%(asctime)s - %(message)s')

def get_json_data(json_file_name) -> dict:
    logging.info(msg='Start')

    with open(json_file_name, 'r', encoding='utf-8') as file:
        data = json.load(file)

        return data


def get_site_list(csv_file_name, data, api_key) -> None:

    geolocator = GoogleV3(api_key=api_key)
    
    with open(csv_file_name, 'w', newline='', encoding='cp1255', errors='replace') as file:
        writer = csv.writer(file)
        headers = ['name', 'peak power', 'address', 'latitude', 'longitude']
        writer.writerow(headers)
        site = data['sites']['site']

        for i in range(len(site)):
            name = site[i]['name']
            peak_power = site[i]['peakPower']
            location = site[i]['location']
            if 'city' in location:
                city = location['city']
            else:
                city = ""
            if 'address' in location:
                street_1 = location['address']
            else:
                street_1 = ""
            if 'address2' in location:
                street_2 = location['address2']
            else:
                street_2 = ""

            try:
                if street_2 != "":
                    address = geolocator.geocode(f'{street_2} {street_1} {city}')
                elif street_1 != "":
                    address = geolocator.geocode(f'{street_1} {city}')
                else:
                    address = geolocator.geocode(f'{city}')

                address_line = [name, peak_power, address.address, address.latitude, address.longitude]
                writer.writerow(address_line)

            except:
                logging.error(msg=f'ERROR finding site in {name} {city} {street_1} {street_2}')

    logging.info(msg='End')

if __name__ == "__main__":
    input = input('Insert Google api key:' )
    google_api_key = input
    data = get_json_data(json_file_name='sites_list.json')
    get_site_list(csv_file_name='sites_locations_.csv', data=data, api_key=google_api_key)