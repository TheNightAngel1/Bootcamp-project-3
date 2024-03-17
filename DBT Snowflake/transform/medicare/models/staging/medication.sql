-- incremental approach with delete + insert

{{
    config(
        materialized="table",
        unique_key=["brand_name"],
        incremental_strategy="delete+insert"
    )
}}

select distinct
    Brnd_Name as brand_name,
    Gnrc_Name as generic_name,
    Opioid_Drug_Flag as opioid_drug_flag,
    Opioid_LA_Drug_Flag as la_opiod_drug_flag,
    Antbtc_Drug_Flag as antibiotic_flag,
    Antpsyct_Drug_Flag as antipsychotic_flag,
    cast(_airbyte_extracted_at as datetime) as last_update

from {{ source('medicare', 'med2021') }}

/*{% if is_incremental() %}
    where last_update > (select max(last_update) from {{ this }} )
{% endif %}*/
