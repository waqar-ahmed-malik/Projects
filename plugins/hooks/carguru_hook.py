from airflow.hooks.base_hook import BaseHook
import requests
import json
import datetime
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from airflow.exceptions import AirflowException


class CarguruHook(BaseHook):
    def __init__(self, app_id: str, auth_token: str):
        retry_strategy = Retry(total=3, status_forcelist=[103,104,105], backoff_factor=60)
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session = requests.Session()
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        self.session = session
        self.params = {
            'appId': app_id,
            'authToken': auth_token
        }


    def get_dealer_vin_insights(self, stats_date: str, dealer_name: str):
        url = 'https://www.cargurus.com/Cars/api/2.0/dealerStatsRequest.action'
        body = {
            "start_date" : stats_date,
            "end_date" : stats_date
        }
        self.params.__setitem__('body', json.dumps(body))
        response = self.session.get(url, params=self.params)
        if response.status_code == 404:
            raise AirflowException('Listing are unavailable for {} on {}'.format(dealer_name, stats_date))
        try:
            response.json()['vin_stats']
        except:
            raise AirflowException('Exception Occurred for {} on {} giving emitting {} status code with response \n {}'.format(dealer_name, stats_date, response.status_code, response.text))
        daily_stats = response.json()['vin_stats']
        for stat in daily_stats:
            stat.__setitem__('date', stats_date)
        return daily_stats

        

        