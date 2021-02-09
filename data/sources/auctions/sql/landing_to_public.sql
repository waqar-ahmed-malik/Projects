DROP TABLE IF EXISTS auctions_temp;


CREATE TEMPORARY TABLE auctions_temp AS
SELECT
vin,
auction_id,
channel AS sale_channel,
CAST(auction_end_date AS TIMESTAMP WITHOUT TIME ZONE) AS end_date,
auction_location AS location,
TO_DATE(sale_date, 'MM/DD/YYYY') AS sale_date,
CAST(auction_start_date AS TIMESTAMP WITHOUT TIME ZONE) AS start_date,
CAST(as_is[1] AS BOOLEAN) AS as_is,
CAST(buy_now_price[1] AS FLOAT)::NUMERIC::MONEY AS buy_now_price,
CAST(certified[1] AS BOOLEAN) AS certified,
comments[1],
STRING_TO_ARRAY(images[1], '||||') AS images,
location_zip[1] AS location_zip,
pickup_region[1] AS pickup_region,
pickup_state[1] AS pickup_state,
pickup_zip[1] AS pickup_zip,
sale_type[1] AS sale_type,
CAST(sale_year[1] AS INT) AS sale_year,
seller_name[1] AS seller_name,
seller_type[1] AS seller_type,
title_status[1] AS title_status,
url[1] AS url,
CASE 
WHEN TRIM(odometer_units[1]) IN ('mi', 'hr') OR odometer_units[1] IS NULL THEN CAST(mileage[1] AS BIGINT) 
ELSE CAST(CAST(mileage[1] AS BIGINT) * 0.621371 AS BIGINT) END AS reading,
engine_description[1] AS engine_description,
exterior_color[1] AS exterior_color,
CASE WHEN TRIM(frame_damage[1]) = '1' THEN CURRENT_DATE ELSE NULL END AS frame_damage,
fuel_type[1] AS fuel_type,
CAST(has_air_conditioner[1] AS BOOLEAN) AS has_air_conditioner,
int_upholstery[1] AS int_upholstery,
interior_color[1] AS interior_color,
make[1] AS make,
model[1] AS model,
CAST(model_year[1] AS INT) AS model_year,
CASE WHEN TRIM(prior_paint[1]) = '1' THEN TRUE ELSE FALSE END AS prior_paint,
CASE WHEN CAST(salvage_title[1] AS BOOLEAN) IS TRUE THEN CURRENT_DATE ELSE NULL END AS salvage_title,
transmission[1] AS transmission
FROM
(
SELECT 
auction_id,
vin,
auction_end_date,
auction_start_date,
channel,
sale_date,
auction_location,
ARRAY_REMOVE(ARRAY_AGG(as_is), NULL) AS as_is,
ARRAY_REMOVE(ARRAY_AGG(buy_now_price), NULL) AS buy_now_price,
ARRAY_REMOVE(ARRAY_AGG(certified), NULL) AS certified,
ARRAY_REMOVE(ARRAY_AGG(comments), NULL) AS comments,
ARRAY_REMOVE(ARRAY_AGG(images), NULL) AS images,
ARRAY_REMOVE(ARRAY_AGG(location_zip), NULL) AS location_zip,
ARRAY_REMOVE(ARRAY_AGG(pickup_region), NULL) AS pickup_region,
ARRAY_REMOVE(ARRAY_AGG(pickup_location_state), NULL) AS pickup_state,
ARRAY_REMOVE(ARRAY_AGG(pickup_location_zip), NULL) AS pickup_zip,
ARRAY_REMOVE(ARRAY_AGG(sale_type), NULL) AS sale_type,
ARRAY_REMOVE(ARRAY_AGG(sale_year), NULL) AS sale_year,
ARRAY_REMOVE(ARRAY_AGG(seller_name), NULL) AS seller_name,
ARRAY_REMOVE(ARRAY_AGG(seller_types), NULL) AS seller_type,
ARRAY_REMOVE(ARRAY_AGG(title_status), NULL) AS title_status,
ARRAY_REMOVE(ARRAY_AGG(vehicle_sale_url), NULL) AS url,
ARRAY_REMOVE(ARRAY_AGG(odometer_units), NULL) AS odometer_units,
ARRAY_REMOVE(ARRAY_AGG(mileage), NULL) AS mileage,
ARRAY_REMOVE(ARRAY_AGG(engine), NULL) AS engine_description,
ARRAY_REMOVE(ARRAY_AGG(exterior_color), NULL) AS exterior_color,
ARRAY_REMOVE(ARRAY_AGG(frame_damage), NULL) AS frame_damage,
ARRAY_REMOVE(ARRAY_AGG(fuel_type), NULL) AS fuel_type,
ARRAY_REMOVE(ARRAY_AGG(has_air_conditioning), NULL) AS has_air_conditioner,
ARRAY_REMOVE(ARRAY_AGG(interior_type), NULL) AS int_upholstery,
ARRAY_REMOVE(ARRAY_AGG(interior_color), NULL) AS interior_color,
ARRAY_REMOVE(ARRAY_AGG(make_id), NULL) AS make,
ARRAY_REMOVE(ARRAY_AGG(model_id), NULL) AS model,
ARRAY_REMOVE(ARRAY_AGG(model_year), NULL) AS model_year,
ARRAY_REMOVE(ARRAY_AGG(prior_paint), NULL) AS prior_paint,
ARRAY_REMOVE(ARRAY_AGG(salvage), NULL) AS salvage_title,
ARRAY_REMOVE(ARRAY_AGG(transmission), NULL) AS transmission
FROM landing.auctions
GROUP BY auction_id, vin, auction_end_date, auction_start_date, channel, sale_date, auction_location
) A;

-- ************************************************ ODOMETER ******************************************************
-- ************************************************ ODOMETER ******************************************************
-- ************************************************ ODOMETER ******************************************************

INSERT INTO public.odometer
(vin, reading, date_taken)
SELECT 
LANDING.vin,
LANDING.reading,
LANDING.sale_date AS date_taken
FROM auctions_temp LANDING
LEFT JOIN
public.odometer OPERATIONAL
ON UPPER(LANDING.vin) = UPPER(OPERATIONAL.vin)
AND LANDING.sale_date = OPERATIONAL.date_taken
WHERE OPERATIONAL.vin IS NULL AND LANDING.vin IS NOT NULL AND LANDING.reading IS NOT NULL;

-- ************************************************ MAKE ******************************************************
-- ************************************************ MAKE ******************************************************
-- ************************************************ MAKE ******************************************************

INSERT INTO public.car_make
(name, id_car_type, date_created, date_updated)
SELECT 
TRIM(UPPER(LANDING.make)) AS make,
1 AS id_car_type,
now() AS date_created,
now() AS date_updated
FROM
auctions_temp LANDING
LEFT JOIN
public.car_make OPERATIONAL
ON TRIM(UPPER(LANDING.make)) = UPPER(OPERATIONAL.name)
WHERE LANDING.make IS NOT NULL AND OPERATIONAL.name IS NULL;

-- ************************************************ MODEL ******************************************************
-- ************************************************ MODEL ******************************************************
-- ************************************************ MODEL ******************************************************

INSERT INTO public.car_model
(id_car_make, name, id_car_type, date_created, date_updated)
SELECT 
LANDING.id_car_make,
LANDING.name,
LANDING.id_car_type,
LANDING.date_created,
LANDING.date_updated
FROM
(
SELECT DISTINCT
MAKE.id_car_make,
TRIM(UPPER(model)) AS name,
1 AS id_car_type,
now() AS date_created,
now() AS date_updated
FROM auctions_temp VEHICLES
INNER JOIN
public.car_make MAKE
ON TRIM(UPPER(VEHICLES.make)) = TRIM(UPPER(MAKE.name))
) LANDING
LEFT JOIN
public.car_model OPERATIONAL
ON UPPER(LANDING.name) = UPPER(OPERATIONAL.name)
AND LANDING.id_car_make = OPERATIONAL.id_car_make
WHERE OPERATIONAL.id_car_make IS NULL AND LANDING.name IS NOT NULL;


-- ************************************************ VEHICLES ******************************************************
-- ************************************************ VEHICLES ******************************************************
-- ************************************************ VEHICLES ******************************************************

DROP TABLE IF EXISTS auctions_vehicles_temp;

CREATE TEMPORARY TABLE auctions_vehicles_temp AS
SELECT DISTINCT
VEHICLES.vin,
VEHICLES.engine_description,
VEHICLES.exterior_color,
VEHICLES.frame_damage,
VEHICLES.fuel_type,
VEHICLES.has_air_conditioner,
VEHICLES.int_upholstery,
VEHICLES.interior_color,
MAKE_MODEL.id_car_model AS model_id,
MAKE_MODEL.id_car_make AS make_id,
VEHICLES.model_year,
VEHICLES.prior_paint,
VEHICLES.salvage_title,
VEHICLES.transmission,
now() AS date_updated,
now() AS date_created
FROM auctions_temp VEHICLES
INNER JOIN
(
SELECT 
MAKE.id_car_make,
MODEL.id_car_model,
MAKE.name AS make,
MODEL.name AS model
FROM public.car_model MODEL
INNER JOIN
public.car_make MAKE
ON MODEL.id_car_make = MAKE.id_car_make
) MAKE_MODEL
ON TRIM(UPPER(VEHICLES.model)) = UPPER(MAKE_MODEL.model)
AND TRIM(UPPER(VEHICLES.make)) = UPPER(MAKE_MODEL.make);

INSERT INTO public.vehicles
(vin, engine_description, exterior_color, frame_damage, fuel_type, has_air_conditioner, int_upholstery, interior_color, model_id, make_id, model_year, prior_paint, salvage_title, transmission, date_updated, date_created)
SELECT DISTINCT
LANDING.vin,
MAX(LANDING.engine_description) AS engine_description,
MAX(LANDING.exterior_color) AS exterior_color,
MAX(LANDING.frame_damage) AS frame_damage,
MAX(LANDING.fuel_type) AS fuel_type,
CAST(MAX(CAST(LANDING.has_air_conditioner AS VARCHAR)) AS BOOLEAN) AS has_air_conditioner,
MAX(LANDING.int_upholstery) AS int_upholstery,
MAX(LANDING.interior_color) AS interior_color,
MAX(LANDING.model_id) AS model_id,
MAX(LANDING.make_id) AS make_id,
MAX(LANDING.model_year) AS model_year,
CAST(MAX(CAST(LANDING.prior_paint AS VARCHAR)) AS BOOLEAN) AS prior_paint,
MAX(LANDING.salvage_title) AS salvage_title,
MAX(LANDING.transmission) AS transmission,
now() AS date_updated,
now() AS date_created
FROM auctions_vehicles_temp LANDING
LEFT JOIN
public.vehicles OPERATIONAL
ON UPPER(LANDING.vin) = UPPER(OPERATIONAL.vin)
WHERE OPERATIONAL.vin IS NULL AND LANDING.vin IS NOT NULL
GROUP BY LANDING.vin;

DROP TABLE IF EXISTS auctions_vehicles_temp;

-- ************************************************ AUCTIONS ******************************************************
-- ************************************************ AUCTIONS ******************************************************
-- ************************************************ AUCTIONS ******************************************************

INSERT INTO public.auctions
(vin, as_is, auction_id, buy_now_price, certified, comments, end_date, images, location, location_zip, pickup_region, pickup_state, pickup_zip, sale_channel, sale_date, sale_type, sale_year, seller_name, seller_type, start_date, title_status, url, created_at)
SELECT DISTINCT
LANDING.vin,
LANDING.as_is,
LANDING.auction_id,
LANDING.buy_now_price,
LANDING.certified,
LANDING.comments,
LANDING.end_date,
LANDING.images,
LANDING.location,
LANDING.location_zip,
LANDING.pickup_region,
LANDING.pickup_state,
LANDING.pickup_zip,
LANDING.sale_channel,
LANDING.sale_date,
LANDING.sale_type,
LANDING.sale_year,
LANDING.seller_name,
LANDING.seller_type,
LANDING.start_date,
LANDING.title_status,
LANDING.url,
now() AS created_at
FROM auctions_temp LANDING
LEFT JOIN
public.auctions OPERATIONAL
ON COALESCE(LANDING.auction_id, '') = COALESCE(OPERATIONAL.auction_id, '')
AND COALESCE(LANDING.vin, '') = COALESCE(OPERATIONAL.vin, '')
AND COALESCE(LANDING.end_date, '2020-01-01 00:00:00 UTC') = COALESCE(OPERATIONAL.end_date, '2020-01-01 00:00:00 UTC')
AND COALESCE(LANDING.start_date, '2020-01-01 00:00:00 UTC') = COALESCE(OPERATIONAL.start_date, '2020-01-01 00:00:00 UTC')
AND COALESCE(LANDING.sale_channel, '') = COALESCE(OPERATIONAL.sale_channel, '')
AND COALESCE(LANDING.location, '') = COALESCE(OPERATIONAL.location, '')
WHERE OPERATIONAL.auction_id IS NULL;