import logging
from requests.structures import CaseInsensitiveDict
from meteocontrol.Api import Api
import requests



class Meteocontrol(Api):
    

    def __init__(self, api_key: str, base_url: str, authorization: str) -> None:
        super().__init__(api_key, base_url)
        self.authorization = authorization
        self.headers = CaseInsensitiveDict()
        self.headers["X-API-KEY"] = self.api_key
        self.headers["Authorization"] = self.authorization


    @property
    def authorization(self) -> str:
        return self._authorization


    @authorization.setter
    def authorization(self, authorization) -> None:
        if isinstance(authorization, str):
            self._authorization = authorization
        else:
            logging.error(msg='Value error - authorization key needs to be a string!', exc_info=True)
            raise SystemExit()


    # Meta Data

    def get_sites(self):
        try:
            sites_list = self.get_data(self.base_url, headers=self.headers)
        except requests.exceptions.RequestException as e:
                raise requests.exceptions.RequestException(e)
        except Exception as e:
                raise Exception(e)
        return sites_list


    def get_site_technical_data(self, site_id):
        try:
            site_technical_data = self.get_data(f"{self.base_url}/{site_id}/technical-data", headers=self.headers)
        except requests.exceptions.RequestException as e:
                raise requests.exceptions.RequestException(e)
        except Exception as e:
                raise Exception(e)
        return site_technical_data


    def get_site_details(self, site_id):
        try:
            site_details = self.get_data(f"{self.base_url}/{site_id}", headers=self.headers)
        except requests.exceptions.RequestException as e:
                raise requests.exceptions.RequestException(e)
        except Exception as e:
                raise Exception(e)
        return site_details


    # TODO: NOT sure if they have more than one meter for a site
    def get_meter(self, site_id):
        try:
            meter = self.get_data(f"{self.base_url}/{site_id}/virtual-meters", headers=self.headers)
        except requests.exceptions.RequestException as e:
                raise requests.exceptions.RequestException(e)
        except Exception as e:
                raise Exception(e)
        return meter


    def get_inverters(self, site_id):
        try:
            inverters = self.get_data(f"{self.base_url}/{site_id}/inverters", headers=self.headers)
        except requests.exceptions.RequestException as e:
                raise requests.exceptions.RequestException(e)
        except Exception as e:
                raise Exception(e)
        return inverters


    def get_inverter_details(self, site_id, inverter_id):
        try:
            inverter_meta = self.get_data(f"{self.base_url}/{site_id}/inverters/{inverter_id}", headers=self.headers)
        except requests.exceptions.RequestException as e:
                raise requests.exceptions.RequestException(e)
        except Exception as e:
                raise Exception(e)
        return inverter_meta


    # Data

    def get_site_data(self, site, date):
        start = f"{date}T00%3A00%3A00%2B02%3A00"
        end = f"{date}T23%3A55%3A00%2B02%3A00"
        site_bulk_url = f"{self.base_url}/{site}/bulk/measurements?from={start}&to={end}"
        try:
            site_data = self.get_data(site_bulk_url, headers=self.headers)
        except requests.exceptions.RequestException as e:
                raise requests.exceptions.RequestException(e)
        except Exception as e:
                raise Exception(e)
        return site_data


def get_meter_data(self, site_id, meter):
    try:
        meter_data = self.get_data(f"{self.base_url}/{site_id}/virtual-meters/{meter}/readings", headers=self.headers)
    except requests.exceptions.RequestException as e:
            raise requests.exceptions.RequestException(e)
    except Exception as e:
            raise Exception(e)
    return meter_data
        