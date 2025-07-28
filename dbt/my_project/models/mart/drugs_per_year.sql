{{ config(materialized='table') }}

with source as (
  select * from {{ source('dev', 'pharmaceutical_data') }}
),

parsed as (
  select
    extract(year from ngaycapsoDangky) as year,
    case 
      when lower(nuocDangky) = 'việt nam' then 'Trong nước'
      else 'Nước ngoài'
    end as loai_thuoc
  from source
  where ngaycapsoDangky is not null
),

grouped as (
  select
    year,
    loai_thuoc,
    count(*) as so_thuoc_dang_ky
  from parsed
  group by year, loai_thuoc
)

select * from grouped
