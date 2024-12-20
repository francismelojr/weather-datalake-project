{{
	config(
		materialized='table',
		schema='silver'
		)
}}


select
	location_id
	,location_name
	,location_region
	,location_country
	,latitude
	,longitude
	,tz_id
from {{ ref('stg_locations') }}