-- models/expired_by_year.sql

WITH base AS (
    SELECT
        ngayhethansodangky
    FROM {{ source('dev', 'pharmaceutical_data') }}
    WHERE ngayhethansodangky IS NOT NULL
),

grouped AS (
    SELECT
        DATE_TRUNC('year', ngayhethansodangky) AS nam_het_han,
        COUNT(*) AS so_luong
    FROM base
    GROUP BY 1
)

SELECT *
FROM grouped
ORDER BY nam_het_han
