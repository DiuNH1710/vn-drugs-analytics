{{ config(materialized='table') }}

with source as (
  select * from {{ source('dev', 'pharmaceutical_data') }}
),

parsed as (
  select
    tencongtysanxuat,
    case 
      when lower(nuocDangky) = 'việt nam' then 'Trong nước'
      else 'Nước ngoài'
    end as loai_thuoc
  from source
),

ranked as (
  select
    tencongtysanxuat,
    loai_thuoc,
    count(*) as so_luong,
    row_number() over (partition by loai_thuoc order by count(*) desc) as rank
  from parsed
  group by tencongtysanxuat, loai_thuoc
)

select * from ranked where rank <= 10
