from airflow.operators import BaseOperator
from airflow.utils.decorators import apply_defaults
from hooks.auctions_hook import AuctionsHook
from airflow.hooks.S3_hook import S3Hook
import csv
import os



def parse_response(total_listings: list) -> list:
    parsed_response = list()
    for listing in total_listings:
        parsed_response_row = dict()
        parsed_response_row.__setitem__("as_is", listing.get("vehicleInformation", {}).get("asIs"))
        parsed_response_row.__setitem__("buy_now_price", listing.get("vehicleInformation", {}).get("buyNowPrice"))
        parsed_response_row.__setitem__("certified", listing.get("vehicleInformation", {}).get("certified"))
        parsed_response_row.__setitem__("comments", listing.get("vehicleInformation", {}).get("comments"))
        parsed_response_row.__setitem__("engine", listing.get("vehicleInformation", {}).get("engine"))
        parsed_response_row.__setitem__("exterior_color", listing.get("vehicleInformation", {}).get("exteriorColor"))
        parsed_response_row.__setitem__("frame_damage", listing.get("vehicleInformation", {}).get("frameDamage"))
        parsed_response_row.__setitem__("fuel_type", listing.get("vehicleInformation", {}).get("fuelType"))
        parsed_response_row.__setitem__("has_air_conditioning", listing.get("vehicleInformation", {}).get("hasAirConditioning"))
        parsed_response_row.__setitem__("images", '||||'.join([image.get('largeUrl') for image in listing.get("vehicleInformation", {}).get("images", []) if image.get('largeUrl') is not None]))
        parsed_response_row.__setitem__("interior_color", listing.get("vehicleInformation", {}).get("interiorColor"))
        parsed_response_row.__setitem__("interior_type", listing.get("vehicleInformation", {}).get("interiorType"))
        parsed_response_row.__setitem__("location_zip", listing.get("vehicleInformation", {}).get("locationZip"))
        parsed_response_row.__setitem__("make_id", listing.get("vehicleInformation", {}).get("make"))
        parsed_response_row.__setitem__("mileage", listing.get("vehicleInformation", {}).get("mileage"))
        parsed_response_row.__setitem__("model_id", listing.get("vehicleInformation", {}).get("model"))
        parsed_response_row.__setitem__("odometer_units", listing.get("vehicleInformation", {}).get("odometerUnits"))
        parsed_response_row.__setitem__("pickup_location_state", listing.get("vehicleInformation", {}).get("pickupLocationState"))
        parsed_response_row.__setitem__("pickup_location_zip", listing.get("vehicleInformation", {}).get("pickupLocationZip"))
        parsed_response_row.__setitem__("pickup_region", listing.get("vehicleInformation", {}).get("pickupRegion"))
        parsed_response_row.__setitem__("prior_paint", listing.get("vehicleInformation", {}).get("priorPaint"))
        parsed_response_row.__setitem__("salvage", listing.get("vehicleInformation", {}).get("salvage"))
        parsed_response_row.__setitem__("seller_types", '||||'.join([seller_type for seller_type in listing.get("vehicleInformation", {}).get("sellerTypes", []) if seller_type is not None]))
        parsed_response_row.__setitem__("title_status", listing.get("vehicleInformation", {}).get("titleStatus"))
        parsed_response_row.__setitem__("transmission", listing.get("vehicleInformation", {}).get("transmission"))
        parsed_response_row.__setitem__("vin", listing.get("vehicleInformation", {}).get("vin"))
        parsed_response_row.__setitem__("model_year", listing.get("vehicleInformation", {}).get("year"))
        parsed_response_row.__setitem__("auction_end_date", listing.get("saleInformation", {}).get("auctionEndDate"))        
        parsed_response_row.__setitem__("auction_id", listing.get("saleInformation", {}).get("auctionId"))        
        parsed_response_row.__setitem__("auction_location", listing.get("saleInformation", {}).get("auctionLocation"))        
        parsed_response_row.__setitem__("auction_start_date", listing.get("saleInformation", {}).get("auctionStartDate"))        
        parsed_response_row.__setitem__("channel", listing.get("saleInformation", {}).get("channel"))        
        parsed_response_row.__setitem__("sale_date", listing.get("saleInformation", {}).get("saleDate"))        
        parsed_response_row.__setitem__("sale_type", listing.get("saleInformation", {}).get("saleType"))        
        parsed_response_row.__setitem__("sale_year", listing.get("saleInformation", {}).get("saleYear"))        
        parsed_response_row.__setitem__("vehicle_sale_url", listing.get("saleInformation", {}).get("vehicleSaleURL"))        
        parsed_response_row.__setitem__("seller_name", listing.get("sellerInformation", {}).get("sellerName"))                        
        parsed_response.append(parsed_response_row)
    return parsed_response


class AuctionsToS3Operator(BaseOperator):
    def __init__(self,
                 s3_conn_id,
                 s3_bucket,
                 s3_key,
                 api_key,
                 sale_date,
                 delimiter,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)

        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.api_key = api_key
        self.sale_date = sale_date
        self.delimiter = delimiter
        

    def execute(self, context):
        s3_conn = S3Hook(self.s3_conn_id)
        auctions_hook = AuctionsHook(self.api_key)
        total_listings = auctions_hook.get_listings(self.sale_date)
        stats = parse_response(total_listings)
        field_names = [key for key, value in stats[0].items()]
        file_name = '/tmp/auctions.csv'
        with open(file_name, 'w') as f:
            csv_writer = csv.DictWriter(f, fieldnames=field_names, delimiter=self.delimiter)
            csv_writer.writeheader()
            for stat in stats:
                row = {}
                for key, value in stat.items():
                    if isinstance(value, str):
                        v = value.replace('\\n', ' ').replace('\\r', ' ').replace('\n', ' ').replace('\r', ' ').replace('\\', ' ').replace(self.delimiter, '_')
                        row.__setitem__(key, v)
                    else:
                        row.__setitem__(key, value)
                csv_writer.writerow(row)
            
        s3_conn.load_file(file_name, self.s3_key, self.s3_bucket, True)
        os.remove(file_name)
    