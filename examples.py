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