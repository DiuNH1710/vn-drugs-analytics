{{ config(materialized='table') }}

SELECT
    COUNT(*) AS tong_so_thuoc
FROM {{ source('dev', 'pharmaceutical_data') }}
WHERE sodangky IS NOT NULL
