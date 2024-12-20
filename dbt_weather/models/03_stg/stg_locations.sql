{{
	config(
		materialized='view',
		schema='bronze'
		)
}}


select distinct
	location_id
	,location_name
	,location_region
	,location_country
	,latitude
	,longitude
	,tz_id
from {{ ref('raw_weather_data') }}