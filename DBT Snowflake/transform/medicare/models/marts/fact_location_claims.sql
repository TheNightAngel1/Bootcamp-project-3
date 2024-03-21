-- incremental approach with delete + insert

{{
    config(
        materialized="table",
        unique_key=["prscrbr_geo_desc" , "brand_name"],
        incremental_strategy="delete+insert"
    )
}}




select distinct
    l.prscrbr_geo_desc, 
    c.brand_name,
    sum(c.total_claims) as total_claims,
    sum(c.total_beneficiaries) as total_beneficiaries,
    sum(c.total_prescribers) as total_prescribers

from {{ ref('claims') }} as c
join {{ ref('location') }} as l on c.prscrbr_geo_cd = l.prscrbr_geo_cd
group by l.prscrbr_geo_desc, c.brand_name
order by l.prscrbr_geo_desc