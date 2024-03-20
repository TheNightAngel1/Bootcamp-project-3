-- incremental approach with delete + insert

{{
    config(
        materialized="table",
        unique_key=["prscrbr_geo_cd"],
        incremental_strategy="delete+insert"
    )
}}

select 
    *
from {{ ref('location') }}

{% if is_incremental() %}
    where last_update > (select max(last_update) from {{ this }} )
{% endif %}

