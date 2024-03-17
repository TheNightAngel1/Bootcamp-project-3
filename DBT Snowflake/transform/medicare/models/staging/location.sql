-- incremental approach with delete + insert

{{
    config(
        materialized="incremental",
        unique_key=["prscrbr_geo_cd"],
        incremental_strategy="delete+insert"
    )
}}

select 
    distinct
        Prscrbr_Geo_Lvl as geography_level,
        Prscrbr_Geo_Cd as prscrbr_geo_cd,
        Prscrbr_Geo_Desc as prscrbr_geo_desc,
        cast(_airbyte_extracted_at as datetime) as last_update

from {{ source('medicare', 'med2021') }}

{% if is_incremental() %}
    where last_update > (select max(last_update) from {{ this }} )
{% endif %}

