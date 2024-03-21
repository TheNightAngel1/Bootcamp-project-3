-- incremental approach with delete + insert

{{
    config(
        materialized="incremental",
        unique_key=["brand_name", "prscrbr_geo_cd", ],
        incremental_strategy="delete+insert"
    )
}}



select 
    l.prscrbr_geo_desc as location,
    l.geography_level,
    m.generic_name,
    c.*,
    m.opioid_drug_flag,
    m.la_opiod_drug_flag,
    m.antibiotic_flag,
    m.antipsychotic_flag


from {{ ref('claims') }} as c
join {{ ref('location') }} as l on c.prscrbr_geo_cd = l.prscrbr_geo_cd
join {{ ref('medication') }} as m on m.brand_name = c.brand_name 
order by l.prscrbr_geo_cd, c.brand_name
