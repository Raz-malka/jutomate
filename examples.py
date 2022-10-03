from SolarEdge import Solaredge
from SolarEdgeAirFlowRunner import SolaredgeAirFlowRunner

# if __name__ == "__main__":
#     base_api = Solaredge("EC6PM19AGTOXAWM0QFTR5UFBH471V8O5","https://monitoringapi.solaredge.com")
#     airflow_runner = SolaredgeAirFlowRunner(base_api,"bse-bronze")
#     airflow_runner.get_sites()
#     airflow_runner.get_inventory("2056256")

if __name__ == "__main__":
    base_api = Solaredge("EC6PM19AGTOXAWM0QFTR5UFBH471V8O5","https://monitoringapi.solaredge.com")
    airflow_runner = SolaredgeAirFlowRunner(base_api,"bse-ingestion")

    #get site list
    sites = airflow_runner.get_sites()
    site_id_list=[]
    for site_dict in sites['sites']['site']:
        site_id_list.append(site_dict.get("id"))
    print(site_id_list)

#     #get inverter list
#     for single_date in daterange(start_date, end_date):
#         print(single_date)
#     print("Thanks you!")


# list_sites     = Variable.get("list_sites", deserialize_json=True)
# def daterange(start_date, end_date):
#     for n in range(int ((end_date - start_date).days)):
#         yield start_date + timedelta(n)
# start_date  =   Variable.get("loop_start_date")
# end_date    =   Variable.get("loop_end_date")
# if (start_date is not None and  end_date is not None):
# 	start_date 	= datetime.datetime.strptime(start_date, '%Y-%m-%d')
# 	end_date 	= datetime.datetime.strptime(end_date, '%Y-%m-%d')    
# for site_id in list_sites:
#     try:
#         list_inverters = Variable.get("site_"+str(site_id)+"_inverters_list", deserialize_json=True)
#     except:
#         list_inverters = []
#     for inverter_id in list_inverters:
#         for single_date in daterange(start_date, end_date):
#             if single_date.date() == datetime.date.today():
#                 print(single_date.date())
#                 print('dan')
#             else: 
#                 print('update_inverters_list_'+str(site_id)+'_'+str(inverter_id)+'_'+str(single_date.date()))
#                 print('raz')


# def daterange(start_date, end_date):
#     for n in range(int ((end_date - start_date).days)):
#         yield start_date + timedelta(n)

# start_date = datetime.datetime.strptime("2022-08-01", '%Y-%m-%d').date()
# end_date = datetime.datetime.strptime('2022-08-11', '%Y-%m-%d').date()
# day_delta = datetime.timedelta(days=1)

# print(start_date)
# print(end_date)
# print(day_delta)

# for single_date in daterange(start_date, end_date):
#     print(single_date)