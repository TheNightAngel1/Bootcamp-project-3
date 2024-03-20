-- incremental approach with delete + insert

{{
    config(
        materialized="table",
        unique_key=["brand_name"],
        incremental_strategy="delete+insert"
    )
}}

select 
   *
from {{ ref('medication') }}

/*{% if is_incremental() %}
    where last_update > (select max(last_update) from {{ this }} )
{% endif %}*/
