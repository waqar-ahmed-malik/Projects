INSERT INTO public."views" ( vdp, srp, source, vin, date_occurred, date_created )
SELECT 
    landing."views_carguru".vdp::int4, 
    landing."views_carguru".srp::int4, 
    landing."views_carguru".source, 
    landing."views_carguru".vin, 
    landing."views_carguru".date_occurred::date, 
    landing."views_carguru".date_entered::timestamp
FROM landing."views_carguru" 
LEFT JOIN public."views" ON 
landing."views_carguru".vin = public."views".vin
AND landing."views_carguru".date_occurred::date = public."views".date_occurred 
AND public."views".source = 'carguru'
WHERE public."views".id IS NULL;



DROP TABLE landing."views_carguru";

