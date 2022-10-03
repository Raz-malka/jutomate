from asyncio.log import logger
from time import sleep
import requests
import logging



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


    def get_json_data(self, url: str, connection_attempts: int=0) -> dict or int:
        # function return dict of json data if data is received properly, else return int of response status
        try:
            session = requests.Session()
            response = session.get(url, verify=False)

        except requests.exceptions.ConnectionError:
            logging.error(msg='Error - No connecting, make sure internet connection is stable', exc_info=True)
            logging.error(msg='Waiting while attempting to reastablish connection...')
            sleep(30)
            if connection_attempts < 5:
                connection_attempts += 1
                data = self.get_json_data(url, connection_attempts)
                return data
            else:
                logging.exception(msg='Error - Too many attempts to reastablish connection!')
            
        if response.status_code == 200:
            data = response.json()
            return data

        elif response.status_code == 403:
            logger.error(msg=f'Error - Fix URL parameters! status code: {response.status_code} | URL: {url}')
        
        elif response.status_code == 429:
            logger.exception(msg=f"Error - Too many requests for this site or overall today! status code: {response.status_code} | URL: {url} | text: {response.text}")

        elif response.status_code == 304:
            logger.exception(msg=f'Error - Unmodified, if a hash attribute was specified! status code: {response.status_code} | URL: {url} | text: {response.text}')
        
        elif response.status_code == 404:
            logger.exception(msg=f'Error - Not found! Seems like this data dose not appear in this site. status code: {response.status_code} | URL: {url} | text: {response.text}')

        elif response.status_code == 400:
            logger.exception(msg=f'Error - Typo in URL! Check it and try again. status code: {response.status_code} | URL: {url}')
        
        return response.status_code