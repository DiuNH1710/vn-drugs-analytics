{{ config(materialized='table') }}

WITH source AS (
  SELECT * FROM {{ source('dev', 'pharmaceutical_data') }}
),

filtered AS (
  SELECT *
  FROM source
  WHERE ngayhethansodangky IS NOT NULL
    AND CURRENT_DATE < ngayhethansodangky
    AND ngayhethansodangky <= CURRENT_DATE + INTERVAL '6 months'
)

SELECT * FROM filtered
