-- TRUNCATE TABLE landing."views_cars_com";


INSERT INTO public.car_make
(name, id_car_type, date_created, date_updated)
SELECT 
DISTINCT TRIM(UPPER(vehiclemake)) AS name,
1 AS id_car_type,
now() AS date_created,
now() AS date_updated
FROM landing.views_cars_com VEHICLES
LEFT JOIN
public.car_make MAKE
ON UPPER(TRIM(VEHICLES.vehiclemake)) = UPPER(TRIM(MAKE.name))
WHERE VEHICLES.vehiclemake IS NOT NULL AND MAKE.name IS NULL;


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
TRIM(UPPER(vehiclemodel)) AS name,
1 AS id_car_type,
now() AS date_created,
now() AS date_updated
FROM landing.views_cars_com VEHICLES
INNER JOIN
public.car_make MAKE
ON TRIM(UPPER(VEHICLES.vehiclemake)) = TRIM(UPPER(MAKE.name))
) LANDING
LEFT JOIN
public.car_model OPERATIONAL
ON TRIM(UPPER(LANDING.name)) = TRIM(UPPER(OPERATIONAL.name))
AND LANDING.id_car_make = OPERATIONAL.id_car_make
WHERE OPERATIONAL.id_car_make IS NULL AND LANDING.name IS NOT NULL;


INSERT INTO public.vehicles
(vin, make_id, model_id, model_year, date_updated, date_created)
SELECT 
LANDING.vin,
LANDING.make_id,
LANDING.model_id,
LANDING.model_year,
LANDING.date_created,
LANDING.date_updated
FROM
(
SELECT DISTINCT
vin,
MAKE_MODEL.id_car_make AS make_id,
MAKE_MODEL.id_car_model AS model_id,
CAST("year"[1] AS INT) AS model_year,
now() AS date_updated,
now() AS date_created
FROM
(
SELECT 
vin,
ARRAY_REMOVE(ARRAY_AGG(vehiclemake), NULL) AS make,
ARRAY_REMOVE(ARRAY_AGG(vehiclemodel),NULL) AS model,
ARRAY_REMOVE(ARRAY_AGG(vehicleyear), NULL) AS YEAR
FROM landing.views_cars_com
GROUP BY vin
) VEHICLES
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
ON UPPER(VEHICLES.model[1]) = UPPER(MAKE_MODEL.model)
AND UPPER(VEHICLES.make[1]) = UPPER(MAKE_MODEL.make)
) LANDING
LEFT JOIN
public.vehicles OPERATIONAL
ON LANDING.vin = OPERATIONAL.vin
WHERE OPERATIONAL.vin IS NULL AND LANDING.vin IS NOT NULL;



INSERT INTO public."views"
(vdp, srp, "source", vin, date_occurred, date_created)
SELECT 
CAST(LANDING.vdp AS INT) AS vdp,
CAST(LANDING.srp AS INT) AS srp,
LANDING."source",
LANDING.vin,
CAST(LANDING.date_occurred AS TIMESTAMP WITHOUT TIME ZONE) AS date_occurred,
CAST(LANDING.date_entered AS TIMESTAMP WITHOUT TIME ZONE) AS date_created
FROM landing.views_cars_com AS LANDING
LEFT JOIN
public."views" AS OPERATIONAL
ON  LANDING.vin = OPERATIONAL.vin
AND LANDING."source" = OPERATIONAL."source" 
AND CAST(LANDING.date_occurred AS TIMESTAMP WITHOUT TIME ZONE) = OPERATIONAL.date_occurred
WHERE OPERATIONAL.id IS NULL;


-- TRUNCATE TABLE landing."views_cars_com";


