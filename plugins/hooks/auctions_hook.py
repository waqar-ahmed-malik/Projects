from airflow.hooks.base_hook import BaseHook
import requests
import json
import datetime
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from airflow.exceptions import AirflowException


class AuctionsHook(BaseHook):
    def __init__(self, api_key: str):
        self.api_key = api_key


    def get_listings(self, sale_date):
        total_listings = list()
        url = 'https://api.manheim.com/isws-basic/listings?api_key={}'.format(self.api_key)
        sale_date = sale_date.strftime('%m/%d/%Y').replace('/', '%2F')
        page_num = 1   
        payload='pageSize=1000&pageNumber={}&SALE_DATE={}'.format(page_num, sale_date)
        # payload='pageSize=1000&pageNumber={}'.format(page_num)    
        headers = {
        'Cache-Control': 'no-cache',
        'Content-Type': 'application/x-www-form-urlencoded'
        }
        response = requests.request("POST", url, headers=headers, data=payload)
        status_code = response.status_code
        response = response.json()
        print("Total Listings: ", response["totalListings"])
        if len(response.get("listings", [])):
            total_listings.extend(response["listings"])
            print("Page: {} | {}".format(page_num, len(response.get("listings", []))))
        # while len(response.get("listings", [])):
        #     page_num += 1
        #     payload='pageSize=1000&pageNumber={}&SALE_DATE={}'.format(page_num, sale_date)
        #     # payload='pageSize=1000&pageNumber={}'.format(page_num)
        #     response = requests.request("POST", url, headers=headers, data=payload)
        #     status_code = response.status_code
        #     response = response.json()
        #     if len(response.get("listings", [])):
        #         total_listings.extend(response["listings"])
        #     print("Page: {} | {}".format(page_num, len(response.get("listings", []))))
            
        return total_listings

        

        