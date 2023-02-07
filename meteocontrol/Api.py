import requests
import logging
from meteocontrol.decorater_retry import retry



class Api:

    '''
    ***** POSSIBLE IMPROVEMENT - ASYNC *****
    '''
    
    def __init__(self, api_key, base_url) -> None:
        self.api_key = api_key
        self.base_url = base_url


    @property
    def api_key(self) -> str:
        return self._api_key


    @property
    def base_url(self):
        return self._base_url


    @api_key.setter
    def api_key(self, api_key) -> None:
        if isinstance(api_key, str):
            self._api_key = api_key
        else:
            logging.error(msg='Value error - API key needs to be a string!', exc_info=True)
            raise SystemExit()


    @base_url.setter
    def base_url(self, base_url):
        if isinstance(base_url, str):
            self._base_url = base_url
        else:
            logging.error(msg='Value error - base URL needs to be a string!', exc_info=True)
            raise SystemExit()


    def error_log(self, res):
        if res.status_code == 403:
            logging.error(msg=f'Error {res.status_code} on {res.url} - Not authorised! / Given time period is too long!\nError message: {res.text}')

        elif res.status_code == 429:
            logging.error(msg=f"Error {res.status_code} on {res.url} - Too many requests for this site or overall today!\nError message: {res.text}")

        elif res.status_code == 304:
            logging.error(msg=f'Error {res.status_code} on {res.url} - Unmodified!\nError message: {res.text}')

        elif res.status_code == 404:
            logging.error(msg=f'Error {res.status_code} on {res.url} - Not found!\nError message: {res.text}')

        elif res.status_code == 400:
            logging.error(msg=f'Error {res.status_code} on {res.url} - Typo in URL!\nError message: {res.text}')


    @retry(Exception, 4, 20)
    def get_data(self, url: str, headers=None) -> dict:
        try:
            response = requests.get(url, verify=False, headers=headers)

        except requests.exceptions.ConnectionError as e:
            raise requests.exceptions.ConnectionError(f"Error - No internet connection!\nexception:{e}")
        
        except requests.exceptions.Timeout as e:
            raise requests.exceptions.Timeout(f"Error - Timeout occurred!\nexception:{e}")

        except Exception as e:
            raise Exception(f'Error getting api data for {url}!\nexception: {e}')

        if response.status_code != 200:
            raise requests.exceptions.RequestException(response)

        return response