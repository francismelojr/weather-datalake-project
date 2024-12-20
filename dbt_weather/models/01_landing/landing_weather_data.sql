{{
	config(
		materialized='view',
		schema='bronze'
		)
}}


select
	md5(concat(location_name, last_updated)) as unique_key
	,md5(location_name) as location_id
	,location_name
	,location_region
	,location_country
	,latitude
	,longitude
	,tz_id
	,localtime_epoch
	,localtime
	,last_updated_epoch
	,last_updated
	,temp_c
	,temp_f
	,is_day
	,condition_text
	,wind_mph
	,wind_kph
	,wind_degree
	,wind_dir
	,pressure_mb
	,pressure_in
	,precip_mm
	,precip_in
	,humidity
	,cloud
	,feelslike_c
	,feelslike_f
	,windchill_c
	,windchill_f
	,heatindex_c
	,heatindex_f
	,dewpoint_c
	,dewpoint_f
	,vis_km
	,vis_miles
	,uv
	,gust_mph
	,gust_kph
	,air_quality_co
	,air_quality_no2
	,air_quality_o3
	,air_quality_so2
	,air_quality_pm2_5
	,air_quality_pm10
	,air_quality_us_epa_index
	,air_quality_gb_defra_index
from delta_scan('s3://weather-datalake-bucket/bronze/raw_weather_data/')
where 1=1
	and last_updated::timestamp > (current_date - interval 4 day)
