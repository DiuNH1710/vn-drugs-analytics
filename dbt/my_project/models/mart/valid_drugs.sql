{{ config(materialized='table') }}

SELECT
    COUNT(*) AS so_thuoc_con_hieu_luc
FROM {{ source('dev', 'pharmaceutical_data') }}
WHERE ngayhethansodangky IS NOT NULL
  AND ngayhethansodangky > CURRENT_DATE
