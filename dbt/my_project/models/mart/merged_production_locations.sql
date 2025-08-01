-- models/merged_production_locations.sql

SELECT
    p.sodangky,
    p.diachisanxuat,
    l.latitude,
    l.longitude,
    l.location
FROM {{ source('dev', 'pharmaceutical_data') }} p
LEFT JOIN {{ source('dev', 'geocoded_manufacturing_addresses') }} l
  ON TRIM(LOWER(p.diachisanxuat)) = TRIM(LOWER(l.diachisanxuat))
WHERE p.diachisanxuat IS NOT NULL
  AND l.latitude IS NOT NULL
  AND l.longitude IS NOT NULL
  AND l.location IS NOT NULL
  
