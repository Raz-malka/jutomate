import json
from time import sleep
import requests
from requests.structures import CaseInsensitiveDict

system = 'ZB5YL'
data_type = 'PR'
start_date = '2022-12-01T00%3A00%3A00%2B02%3A00' 
end_date = '2022-12-01T23%3A55%3A00%2B02%3A00'
meter_id = '123871'

# Session
session = 'https://api.meteocontrol.de/v2/session'

# Systems
systems_abbreviations = 'https://api.meteocontrol.de/v2/systems/abbreviations' # data types
all_measurements = f"https://api.meteocontrol.de/v2/systems/abbreviations/{data_type}/measurements?from={start_date}&to={end_date}"
systems = 'https://api.meteocontrol.de/v2/systems' # meta
system_details = f'https://api.meteocontrol.de/v2/systems/{system}' # meta
system_technical_data = f'https://api.meteocontrol.de/v2/systems/{system}/technical-data' # meta
systems_bulk = f'https://api.meteocontrol.de/v2/systems/{system}/bulk/measurements?from={start_date}&to={end_date}'

# Virtual meters
virtual_meters = f'https://api.meteocontrol.de/v2/systems/{system}/virtual-meters'
single_virtual_meter = f'https://api.meteocontrol.de/v2/systems/{system}/virtual-meters/{meter_id}'
latest_reading = f'https://api.meteocontrol.de/v2/systems/{system}/virtual-meters/{meter_id}/readings'
readings = f'https://api.meteocontrol.de/v2/systems/{system}/virtual-meters/{meter_id}/readings?from={start_date}&to={end_date}&type=all'
virtual_meter_bulk = f'https://api.meteocontrol.de/v2/systems/{system}/bulk/measurements?from={start_date}&to={end_date}'

# Basics
basics_abbreviations = f'https://api.meteocontrol.de/v2/systems/{system}/basics/abbreviations'
basics_single_abbreviation = f'https://api.meteocontrol.de/v2/systems/{system}/basics/abbreviations/E_Z_EVU'
measurements = f'https://api.meteocontrol.de/v2/systems/{system}/basics/abbreviations/E_Z_EVU/measurements?from=2022-01-01T00%3A00%3A00%2B02%3A00&to=2022-01-01T12%3A00%3A00%2B02%3A00'
# bulk_measurements = f'https://api.meteocontrol.de/v2/systems/{system}/basics/bulk/measurements?from=2016-11-01T10:00:00+02:00&to=2016-11-01T10:05:00+02:00'

# Calculations
calculations = f'https://api.meteocontrol.de/v2/systems/{system}/calculations/abbreviations'
calculations_single_abbreviation = f'https://api.meteocontrol.de/v2/systems/{system}/calculations/abbreviations/PR'

# Forecasts
yield_projection = f'https://api.meteocontrol.de/v2/systems/{system}/forecasts/yield/specific-energy?from=2016-10-01T00:00:00+02:00&to=2016-12-31T23:59:59+01:00'
forcast = f'https://api.meteocontrol.de/v2/systems/{system}/forecasts/forecast?format=json&hours_into_future=1&timezone=UTC&resolution=fifteen-minutes'

# Meters
all_meters = f'https://api.meteocontrol.de/v2/systems/{system}/meters'
single_meter = f'https://api.meteocontrol.de/v2/systems/{system}/meters/12345'
meter_abbreviations = f'https://api.meteocontrol.de/v2/systems/{system}/meters/12345/abbreviations'
meter_single_abbreviations = f'https://api.meteocontrol.de/v2/systems/{system}/meters/12345/abbreviations/M_AC_E_EXP'
meter_measurements = f'https://api.meteocontrol.de/v2/systems/{system}/meters/12345/abbreviations/E/measurements?from=2016-11-01T11:00:00+02:00&to=2016-11-01T11:15:00+02:00'
meter_bulk = f'https://api.meteocontrol.de/v2/systems/{system}/meters/bulk/measurements?from=2016-11-01T10:00:00+02:00&to=2016-11-01T10:05:00+02:00'

# Sensors
all_sensors = f'https://api.meteocontrol.de/v2/systems/{system}/sensors'
single_sensor = f'https://api.meteocontrol.de/v2/systems/{system}/sensors/10001'
sensor_abbreviations = f'https://api.meteocontrol.de/v2/systems/{system}/sensors/10001/abbreviations'
sensor_single_abbreviation = f'https://api.meteocontrol.de/v2/systems/{system}/sensors/10001/abbreviations/SRAD'
sensor_measurements = f'https://api.meteocontrol.de/v2/systems/{system}/sensors/10001/abbreviations/SRAD/measurements?from=2016-10-21T20:00:00+02:00&to=2016-10-21T20:05:00+02:00'
sensor_bulk = f'https://api.meteocontrol.de/v2/systems/{system}/sensors/bulk/measurements?from=2016-10-21T20:00:00+02:00&to=2016-10-21T20:05:00+02:00'

# Status
status_devices = f'https://api.meteocontrol.de/v2/systems/{system}/statuses'
single_status = f'https://api.meteocontrol.de/v2/systems/{system}/statuses/10001'
status_abbreviations = f'https://api.meteocontrol.de/v2/systems/{system}/statuses/10001/abbreviations'
status_single_abbreviation = f'https://api.meteocontrol.de/v2/systems/{system}/statuses/10001/abbreviations/STATE'
status_measurements = f'https://api.meteocontrol.de/v2/systems/{system}/statuses/10001/abbreviations/STATE/measurements?from=2020-10-01T00:00:00+02:00&to=2020-10-01T00:05:00+02:00'
status_bulk = f'https://api.meteocontrol.de/v2/systems/{system}/statuses/bulk/measurements?from=2020-10-01T00:00:00+02:00&to=2020-10-01T00:05:00+02:00'

# Inverters
all_inverters = f'https://api.meteocontrol.de/v2/systems/{system}/inverters'
single_inverter = f'https://api.meteocontrol.de/v2/systems/{system}/inverters/Id184510.3'
inverter_abbreviation = f'https://api.meteocontrol.de/v2/systems/{system}/inverters/Id184510.3/abbreviations'
inverter_single_abbreviation = f'https://api.meteocontrol.de/v2/systems/{system}/inverters/Id184510.3/abbreviations/P_AC'
inverter_measurements = f'https://api.meteocontrol.de/v2/systems/{system}/inverters/Id184510.3/abbreviations/measurements?from={start_date}&to={end_date}'
inverters_bulk = f'https://api.meteocontrol.de/v2/systems/{system}/inverters/bulk/measurements?from={start_date}&to={end_date}'

# Stringboxes
all_stringboxes = f'https://api.meteocontrol.de/v2/systems/{system}/stringboxes'
single_stringboxes = f'https://api.meteocontrol.de/v2/systems/{system}/stringboxes/20001'
stringboxes_abbreviations = f'https://api.meteocontrol.de/v2/systems/{system}/stringboxes/20001/abbreviations'
single_stringboxes_abbreviations = f'https://api.meteocontrol.de/v2/systems/{system}/stringboxes/20001/abbreviations/I1'
stringbox_measurements = f'https://api.meteocontrol.de/v2/systems/{system}/stringboxes/20001/abbreviations/I1/measurements?from=2016-10-21T11:00:00+02:00&to=2016-10-21T11:05:00+02:00'
stringbox_bulk = f'https://api.meteocontrol.de/v2/systems/{system}/stringboxes/bulk/measurements?from=2016-11-01T23:00:00+02:00&to=2016-11-01T23:05:00+02:00'

# Batteries
all_batteries = f'https://api.meteocontrol.de/v2/systems/{system}/batteries'
single_battery = f'https://api.meteocontrol.de/v2/systems/{system}/batteries/145146'
battery_abbreviations = f'https://api.meteocontrol.de/v2/systems/{system}/batteries/145146/abbreviations'
battery_single_abbreviation = f'https://api.meteocontrol.de/v2/systems/{system}/batteries/145146/abbreviations/B_CHARGE_LEVEL'
battery_measurements = f'https://api.meteocontrol.de/v2/systems/{system}/batteries/145146/abbreviations/B_CHARGE_LEVEL/measurements?from=2016-11-01T11:00:00+02:00&to=2016-11-01T11:05:00+02:00'
battery_bulk = f'https://api.meteocontrol.de/v2/systems/{system}/batteries/bulk/measurements?from=2016-10-10T11:00:00+02:00&to=2016-10-10T11:15:00+02:00'

# Power plant controllers
all_power_plant_controllers = f'https://api.meteocontrol.de/v2/systems/{system}/power-plant-controllers'
single_power_plant_controllers = f'https://api.meteocontrol.de/v2/systems/{system}/power-plant-controllers/163784'
power_plant_controllers_abbreviations = f'https://api.meteocontrol.de/v2/systems/{system}/power-plant-controllers/163784/abbreviations'
power_plant_controllers_single_abbreviations = f'https://api.meteocontrol.de/v2/systems/{system}/power-plant-controllers/145146/abbreviations/PPC_P_AC'
power_plant_controllers_measurements = f'https://api.meteocontrol.de/v2/systems/{system}/power-plant-controllers/163784/abbreviations/PPC_P_AC/measurements?from=2016-10-29T11:00:00+02:00&to=2016-10-29T11:05:00+02:00'
power_plant_controllers_measurements_bulk = f'https://api.meteocontrol.de/v2/systems/{system}/power-plant-controllers/bulk/measurements?from=2016-10-29T18:00:00+02:00&to=2016-10-29T18:15:00+02:00'

# Trackers
all_trackers = f'https://api.meteocontrol.de/v2/systems/{system}/trackers'
single_tracker = f'https://api.meteocontrol.de/v2/systems/{system}/trackers/30001'
trackers_abbreviation = f'https://api.meteocontrol.de/v2/systems/{system}/trackers/30001/abbreviations'
trackers_single_abbreviation = f'https://api.meteocontrol.de/v2/systems/{system}/trackers/30001/abbreviations/ELEVATION'
trackers_measurements = f'https://api.meteocontrol.de/v2/systems/{system}/trackers/30001/abbreviations/ELEVATION/measurements?from=2016-11-27T09:00:00+01:00&to=2016-11-27T10:00:00+01:00'
trackers_measurements_bulk = f'https://api.meteocontrol.de/v2/systems/{system}/trackers/bulk/measurements?from=2016-11-27T09:00:00+01:00&to=2016-11-27T10:00:00+01:00'

# Responsibilities
information_of_responsibilities = f'https://api.meteocontrol.de/v2/systems/{system}/responsibilities'

# Users
user_list = f'https://api.meteocontrol.de/v2/systems/{system}/users'
single_user_info = f'https://api.meteocontrol.de/v2/systems/{system}/users/123'
single_user_information_by_its_username = f'https://api.meteocontrol.de/v2/systems/{system}/users?username=user1.name'

# Pictures
pictures = f'https://api.meteocontrol.de/v2/systems/{system}/picture'

# Tickets
all_tickets = f'https://api.meteocontrol.de/v2/tickets?includeInReports=no&severity=normal&priority=normal&status=open&assignee=Yotam_kst&systemKey={system}&lastChangedAt[from]=2022-01-01T00%3A00%3A00%2B02%3A00&lastChangedAt[to]=2022-01-01T12%3A00%3A00%2B02%3A00&createdAt[from]=2022-01-01T00%3A00%3A00%2B02%3A00&createdAt[to]=2022-01-01T12%3A00%3A00%2B02%3A00&rectifiedAt[from]=2022-01-01T00%3A00%3A00%2B02%3A00&rectifiedAt[to]=2022-01-01T12%3A00%3A00%2B02%3A00'
ticket_causes = f'https://api.meteocontrol.de/v2/tickets/causes'
single_ticket = f'https://api.meteocontrol.de/v2/tickets/123'
all_attachments = f'https://api.meteocontrol.de/v2/tickets/123/attachments'
single_attachment = f'https://api.meteocontrol.de/v2/tickets/123/attachments/1234'
all_histories = f'https://api.meteocontrol.de/v2/tickets/123/histories'

# Alarms
all_alarms = f'https://api.meteocontrol.de/v2/alarms?severity=high,critical&status=open&systemKey={system}&lastChangedAt[from]=2022-01-01T00%3A00%3A00%2B02%3A00&lastChangedAt[to]=2022-01-01T12%3A00%3A00%2B02%3A00'
single_alarm = f'https://api.meteocontrol.de/v2/alarms/789'



#url = "https://api.meteocontrol.de/v2/session"
headers = CaseInsensitiveDict()
headers["X-API-KEY"] = "743db7bfb9e21c88ac001b742e84b64361fe1a66ba06f84a75885a3a5bff1deb"
headers["Authorization"] = "Basic WW90YW1fa3N0OmthY28xMjM0NTY="

abrvs = [
    "COS_PHI",
    "E_DAY",
    "E_INT",
    "E_INT_N",
    "E_TOTAL",
    "ERROR3",
    "ERROR4",
    "ERROR5",
    "ERROR6",
    "F_AC",
    "I_AC",
    "I_AC1",
    "I_AC2",
    "I_AC3",
    "I_DC",
    "P_AC",
    "P_AC_N",
    "P_DC",
    "Q_AC",
    "QS_CI",
    "QS_RX",
    "QS_TX",
    "S_AC",
    "STATE2",
    "T_WR1",
    "T_WR2",
    "U_AC",
    "U_AC_L1L2",
    "U_AC_L2L3",
    "U_AC_L3L1",
    "U_AC1",
    "U_AC2",
    "U_AC3",
    "U_DC"
]
new = {}
# for a in abrvs:
#     sleep(7)
#     url = f'https://api.meteocontrol.de/v2/systems/{system}/inverters/Id184510.3/abbreviations/{a}'
#     res = requests.get(inverters_bulk, headers=headers, verify=False)
#     new[a] = res.json()
# E_DAY = total energy for the day! מצטבר לאורך היום
# E_INT = energy producef every 5 min (interval)
# E_TOTAL = total energy until this day (updates along intrvals)

'ZB5YL'
'Id152142.2'
'2022-11-01 04:00:00'

#print(res.text)
res = requests.get(system_details, headers=headers, verify=False)
with open('data_bulk.json', 'w') as file:
    data = res.json()
    json.dump(new, file, indent=4, ensure_ascii=False)

# TODO:
# systems
#
# inverters_bulk


# curl request:
'''
curl -X GET https://api.meteocontrol.de/v2/session
--compressed 
-u Yotam_kst:kaco123456
-H "X-API-KEY: 743db7bfb9e21c88ac001b742e84b64361fe1a66ba06f84a75885a3a5bff1deb"
'''