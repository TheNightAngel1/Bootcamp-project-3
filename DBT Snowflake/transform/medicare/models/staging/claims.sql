-- incremental approach with delete + insert

{{
    config(
        materialized="table",
        unique_key=["brand_name", "prscrbr_geo_cd" ],
        incremental_strategy="delete+insert"
    )
}}

select 
    Brnd_Name as brand_name,
    coalesce(Prscrbr_Geo_Cd,'0') as prscrbr_geo_cd,
    cast(replace(Tot_Prscrbrs, ',','') as int) as total_prescribers,
    cast(replace(Tot_Clms, ',','') as int) as total_claims,
    cast(replace(Tot_30day_Fills, ',','') as int) as total_30_day_fills,
    cast(replace(Tot_Drug_Cst, ',','') as float) as Total_drug_cost,
    cast(replace(Tot_Benes,',','') as int) as total_beneficiaries, 
    GE65_Sprsn_Flag as ge65_suppress_flag, 
    cast(replace(GE65_Tot_Clms, ',','') as int) as ge65_total_claims,
    cast(replace(GE65_Tot_30day_Fills, ',','')  as int) as ge65_tot_30_day_refills,
    cast(replace(GE65_Tot_Drug_Cst, ',','')  as float) as ge65_tot_drug_cost,
    GE65_Bene_Sprsn_Flag as ge65_ben_sup_flag,
    cast(replace(GE65_Tot_Benes, ',','')  as int) as ge65_total_beneficiaries,
    cast(replace(LIS_Bene_Cst_Shr, ',','')  as float) as lis_ben_cost_share,
    cast(replace(NonLIS_Bene_Cst_Shr, ',','')  as float) as nonlis_ben_cost_share,
    cast(_airbyte_extracted_at as datetime) as last_update

from {{ source('medicare', 'med2021') }}

{% if is_incremental() %}
    where last_update > (select max(last_update) from {{ this }} )
{% endif %}