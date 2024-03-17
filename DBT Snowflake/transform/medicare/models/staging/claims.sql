-- incremental approach with delete + insert

{{
    config(
        materialized="incremental",
        unique_key=["brand_name", "prscrbr_geo_cd", ],
        incremental_strategy="delete+insert"
    )
}}

select 
    Brnd_Name as brand_name,
    coalesce(Prscrbr_Geo_Cd,'0') as prscrbr_geo_cd,
    Tot_Prscrbrs as total_prescribers,
    Tot_Clms as total_claims,
    Tot_30day_Fills	as total_30_day_fills,
    Tot_Drug_Cst as Total_drug_cost,
    Tot_Benes as total_beneficiaries, 
    GE65_Sprsn_Flag as ge65_suppress_flag, 
    GE65_Tot_Clms as ge65_total_claims,
    GE65_Tot_30day_Fills as ge65_tot_30_day_refills,
    GE65_Tot_Drug_Cst as ge65_tot_drug_cost,
    GE65_Bene_Sprsn_Flag as ge65_ben_sup_flag,
    GE65_Tot_Benes as ge65_total_beneficiaries,
    LIS_Bene_Cst_Shr as lis_ben_cost_share,
    NonLIS_Bene_Cst_Shr as nonlis_ben_cost_share,
    cast(_airbyte_extracted_at as datetime) as last_update

from {{ source('medicare', 'med2021') }}

{% if is_incremental() %}
    where last_update > (select max(last_update) from {{ this }} )
{% endif %}