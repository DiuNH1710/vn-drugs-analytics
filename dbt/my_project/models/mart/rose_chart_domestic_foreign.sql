{{ config(materialized='table') }}

WITH base AS (
    SELECT
        phanloaithuocenum,
        CASE
            WHEN nuocsanxuat = 'Việt Nam' THEN 'Trong nước'
            ELSE 'Nước ngoài'
        END AS xuatxu
    FROM {{ source('dev', 'pharmaceutical_data') }}
    WHERE phanloaithuocenum IS NOT NULL
      AND nuocsanxuat IS NOT NULL
),

counts AS (
    SELECT
        phanloaithuocenum,
        xuatxu,
        COUNT(*) AS so_luong
    FROM base
    GROUP BY phanloaithuocenum, xuatxu
),

final AS (
    SELECT
        phanloaithuocenum,
        CASE phanloaithuocenum
            WHEN 1 THEN 'Hóa dược'
            WHEN 2 THEN 'Dược liệu'
            WHEN 3 THEN 'Vắc xin'
            WHEN 4 THEN 'Sinh phẩm'
            WHEN 5 THEN 'Nguyên liệu làm thuốc'
            WHEN 6 THEN 'Thuốc gia công'
            WHEN 7 THEN 'Thuốc chuyển giao công nghệ'
            ELSE 'Không xác định'
        END AS ten_loai_thuoc,
        xuatxu,
        so_luong
    FROM counts
)

SELECT *
FROM final
ORDER BY phanloaithuocenum, xuatxu
