{{ config(materialized='table') }}

WITH pharmaceutical_data AS (
    SELECT *
    FROM {{ source('dev', 'pharmaceutical_data') }}
    WHERE diachisanxuat IS NOT NULL
),

geocoded_deduped AS (
    SELECT DISTINCT ON (TRIM(LOWER(diachisanxuat)))
        diachisanxuat,
        latitude,
        longitude,
        location,
        to_json(
            jsonb_build_object(
                'type', 'Feature',
                'geometry', jsonb_build_object(
                    'type', 'Point',
                    'coordinates', jsonb_build_array(longitude, latitude)
                
                ),
                'properties', jsonb_build_object()
            )
        )::text AS geometry,
        CAST(
            jsonb_build_object(
                'type', 'Feature',
                'geometry', jsonb_build_object(
                    'type', 'Point',
                    'coordinates', jsonb_build_array(longitude, latitude)
                ),
                'properties', jsonb_build_object()
            ) AS TEXT
        ) AS geometry_text

    FROM {{ source('dev', 'geocoded_manufacturing_addresses') }}
    WHERE latitude IS NOT NULL
      AND longitude IS NOT NULL
      AND location IS NOT NULL
)

SELECT
    p.sodangky,
    p.tencongtysanxuat,
    p.diachisanxuat,
    p.nuocsanxuat,
    g.latitude,
    g.longitude,
    g.location, 
    g.geometry, 
    g.geometry_text
FROM pharmaceutical_data p
LEFT JOIN geocoded_deduped g
    ON TRIM(LOWER(p.diachisanxuat)) = TRIM(LOWER(g.diachisanxuat))
