-- incremental approach with delete + insert

{{
    config(
        materialized="incremental",
        unique_key=["brand_name", "prscrbr_geo_cd", ],
        incremental_strategy="delete+insert"
    )
}}

select 
    *
from {{ ref('claims') }}

{% if is_incremental() %}
    where last_update > (select max(last_update) from {{ this }} )
{% endif %}