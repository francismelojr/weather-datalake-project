{{
	config(
		materialized='incremental',
		schema='bronze',
        unique_key='unique_key'
		)
}}

select
    *
from {{ ref('landing_weather_data') }}

{% if is_incremental() %}

where (last_updated::timestamp >= (select max(last_updated::timestamp) from {{ this }}))

{% endif %}