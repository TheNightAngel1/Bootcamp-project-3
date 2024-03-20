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
    m.opioid_drug_flag,
    m.la_opiod_drug_flag
  
from {{ ref('claims') }} as c
join {{ ref('medication') }} as m on m.brand_name = c.brand_name 
where m.opioid_drug_flag = 'Y' or m.la_opiod_drug_flag = 'Y'
order by c.brand_name