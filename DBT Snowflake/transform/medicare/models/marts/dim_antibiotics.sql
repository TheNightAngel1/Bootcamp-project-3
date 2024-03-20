-- incremental approach with delete + insert

{{
    config(
        materialized="incremental",
        unique_key=["brand_name"],
        incremental_strategy="delete+insert"
    )
}}

select
    c.brand_name, 
    m.generic_name,
    m.antibiotic_flag
  
from {{ ref('claims') }} as c
join {{ ref('medication') }} as m on m.brand_name = c.brand_name 
where m.antibiotic_flag = 'Y'
order by c.brand_name