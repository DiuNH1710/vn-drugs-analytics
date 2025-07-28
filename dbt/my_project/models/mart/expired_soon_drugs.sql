{{ config(materialized='table') }}

with source as (
  select * from {{ source('dev', 'pharmaceutical_data') }}
),

filtered as (
  select *
  from source
  where ngayhethansodangky is not null
    and ngayhethansodangky <= current_date + interval '6 months'
)

select * from filtered
