from airflow.hooks.base_hook import BaseHook
import requests
import json
import datetime
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from airflow.exceptions import AirflowException
import logging


logging.getLogger().setLevel(logging.INFO)

class CarsComHook(BaseHook):
    def __init__(self, api_key: str):
        self.api_key = api_key


    def get_vehicle_report(self, customer_id: int, date: str) -> list:
        date = datetime.datetime.strptime(date, '%Y-%m-%d')
        stats = []   
        url = 'https://api.cars.com/DealerMetricsService/1.0/rest/reports/contactdetails/totalmetrics'
        params = {
            'customerId': customer_id,
            'day': date.day,
            'month': date.month,
            'year': date.year,
            'start': 1,
            'pageSize': 100,
            'apikey': self.api_key
        }
        while True:
            response = requests.get(url, params=params)
            if response.status_code == 404:
                if params['start'] == 1:
                    raise AirflowException('Listings Not Available for {} on {}'.format(customer_id, date))
                break
            if response.status_code == 401:
                raise AirflowException('Unauthorized Request for {} on {}'.format(customer_id, date))
            try:
                response.json()['TotalMetricsResult']['vehicleDetailsResult']
            except:
                raise AirflowException('Error Occurred for {} on {} emitting {} status code and response \n {}'.format(customer_id, date, response.status_code, response.text))
            for vehicle_details in response.json()['TotalMetricsResult']['vehicleDetailsResult']:
                if vehicle_details.get('vehicleInfo') is None:
                    continue
                row = OrderedDict()
                try:
                    row.__setitem__('date', date)
                    row.__setitem__('classifiedAdId', vehicle_details.get('vehicleInfo', {}).get('classifiedAdId'))
                    row.__setitem__('stockNumber', vehicle_details.get('vehicleInfo', {}).get('stockNumber'))
                    row.__setitem__('vehicleYear', vehicle_details.get('vehicleInfo', {}).get('vehicleYear'))
                    row.__setitem__('vehicleMake', vehicle_details.get('vehicleInfo', {}).get('vehicleMake'))
                    row.__setitem__('vehicleModel', vehicle_details.get('vehicleInfo', {}).get('vehicleModel'))
                    row.__setitem__('vin', vehicle_details.get('vehicleInfo', {}).get('vin'))
                    row.__setitem__('stockType', vehicle_details.get('vehicleInfo', {}).get('stockType'))
                    row.__setitem__('vehicleStatus', vehicle_details.get('vehicleInfo', {}).get('vehicleStatus'))
                    row.__setitem__('listingPrice', vehicle_details.get('vehicleInfo', {}).get('listingPrice'))
                    row.__setitem__('numberOfPhotos', vehicle_details.get('vehicleInfo', {}).get('numberOfPhotos'))
                    row.__setitem__('sellerNotes', vehicle_details.get('vehicleInfo', {}).get('sellerNotes'))
                    row.__setitem__('currentAge', vehicle_details.get('vehicleInfo', {}).get('currentAge'))
                    row.__setitem__('deletedAge', vehicle_details.get('vehicleInfo', {}).get('deletedAge'))
                    row.__setitem__('deletedLastLeadType', vehicle_details.get('vehicleInfo', {}).get('deletedLastLeadType'))
                    row.__setitem__('deletedLastLeadDateTime', vehicle_details.get('vehicleInfo', {}).get('deletedLastLeadDateTime'))
                    row.__setitem__('searchViews', vehicle_details.get('contactSummary', {}).get('searchViews'))
                    row.__setitem__('detailViews', vehicle_details.get('contactSummary', {}).get('detailViews'))
                    row.__setitem__('conversionRate', vehicle_details.get('contactSummary', {}).get('conversionRate'))
                    row.__setitem__('totalContacts', vehicle_details.get('contactSummary', {}).get('totalContacts'))
                    stats.append(row)
                except Exception:
                    print(vehicle_details)
            params['start'] += 1
        
        if len(stats) == 0:
            logging.info("Listings not available for {}".format(date))
        return stats
