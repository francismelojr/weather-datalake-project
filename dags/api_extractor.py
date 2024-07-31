from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from datetime import datetime
from deltalake import write_deltalake
import requests
import boto3
import json
import pandas as pd
import pyarrow as pa

default_args = {
    'owner': 'Francisco Santos',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 28),
    'retries': 0,
}

def fetch_api():
    cities = [
    'London', 'New York', 'Tokyo', 'Paris', 'Berlin', 'Madrid', 'Rome', 'Beijing', 'Moscow', 'Sydney',
    'Los Angeles', 'Toronto', 'Hong Kong', 'Singapore', 'Dubai', 'Bangkok', 'Istanbul', 'Seoul', 'Mumbai', 'São Paulo',
    'Jakarta', 'Mexico City', 'Buenos Aires', 'Cairo', 'Shanghai', 'Lagos', 'Kolkata', 'Tehran', 'Lima', 'Bangkok',
    'Karachi', 'Manila', 'Rio de Janeiro', 'Guangzhou', 'Kinshasa', 'Lahore', 'Chengdu', 'Bogotá', 'Ho Chi Minh City', 'Chennai',
    'Baghdad', 'Bangalore', 'Hyderabad', 'London', 'Shenzhen', 'Wuhan', 'Hong Kong', 'Ahmedabad', 'Kuala Lumpur', 'Philadelphia',
    'San Antonio', 'San Diego', 'Dallas', 'San Jose', 'Austin', 'Jacksonville', 'Fort Worth', 'Columbus', 'San Francisco', 'Charlotte',
    'Indianapolis', 'Seattle', 'Denver', 'Washington', 'Boston', 'El Paso', 'Nashville', 'Detroit', 'Oklahoma City', 'Portland',
    'Las Vegas', 'Memphis', 'Louisville', 'Baltimore', 'Milwaukee', 'Albuquerque', 'Tucson', 'Fresno', 'Sacramento', 'Mesa',
    'Kansas City', 'Atlanta', 'Omaha', 'Raleigh', 'Miami', 'Long Beach', 'Virginia Beach', 'Oakland', 'Minneapolis', 'Tulsa',
    'Arlington', 'New Orleans', 'Wichita', 'Cleveland', 'Tampa', 'Bakersfield', 'Aurora', 'Honolulu', 'Anaheim', 'Santa Ana'
    ]

    api_key = Variable.get("weather_api_key")

    for city in cities:
        url = (
            f'http://api.weatherapi.com/v1/current.json?key={api_key}&q={city}&aqi=yes'
        )
        response = requests.get(url)
        data = response.json()

        s3 = boto3.client('s3')
        s3.put_object(Bucket='weather-datalake-bucket',
                      Key=f"landing/{city}-{datetime.now().strftime('%d-%m-%Y-%H-%M-%S')}.json",
                      Body=json.dumps(data))


def landing_to_bronze():
    ACCESS_ID = Variable.get("aws_access_id")
    ACCESS_KEY = Variable.get("aws_access_key")

    s3 = boto3.client('s3')
    bucket = 'weather-datalake-bucket'

    data_list = []
    response = s3.list_objects_v2(Bucket=bucket, Prefix='landing')

    if 'Contents' in response:
        for obj in response['Contents']:
            object_key = obj['Key']

            if object_key.endswith('.json'):
                json_obj = s3.get_object(Bucket=bucket, Key=object_key)
                json_data = json.loads(json_obj['Body'].read().decode('utf-8'))

                flat_data = {
                    'location_name': json_data['location']['name'],
                    'location_region': json_data['location']['region'],
                    'location_country': json_data['location']['country'],
                    'latitude': json_data['location']['lat'],
                    'longitude': json_data['location']['lon'],
                    'tz_id': json_data['location']['tz_id'],
                    'localtime_epoch': json_data['location']['localtime_epoch'],
                    'localtime': json_data['location']['localtime'],
                    'last_updated_epoch': json_data['current']['last_updated_epoch'],
                    'last_updated': json_data['current']['last_updated'],
                    'temp_c': json_data['current']['temp_c'],
                    'temp_f': json_data['current']['temp_f'],
                    'is_day': json_data['current']['is_day'],
                    'condition_text': json_data['current']['condition']['text'],
                    'condition_icon': json_data['current']['condition']['icon'],
                    'condition_code': json_data['current']['condition']['code'],
                    'wind_mph': json_data['current']['wind_mph'],
                    'wind_kph': json_data['current']['wind_kph'],
                    'wind_degree': json_data['current']['wind_degree'],
                    'wind_dir': json_data['current']['wind_dir'],
                    'pressure_mb': json_data['current']['pressure_mb'],
                    'pressure_in': json_data['current']['pressure_in'],
                    'precip_mm': json_data['current']['precip_mm'],
                    'precip_in': json_data['current']['precip_in'],
                    'humidity': json_data['current']['humidity'],
                    'cloud': json_data['current']['cloud'],
                    'feelslike_c': json_data['current']['feelslike_c'],
                    'feelslike_f': json_data['current']['feelslike_f'],
                    'windchill_c': json_data['current']['windchill_c'],
                    'windchill_f': json_data['current']['windchill_f'],
                    'heatindex_c': json_data['current']['heatindex_c'],
                    'heatindex_f': json_data['current']['heatindex_f'],
                    'dewpoint_c': json_data['current']['dewpoint_c'],
                    'dewpoint_f': json_data['current']['dewpoint_f'],
                    'vis_km': json_data['current']['vis_km'],
                    'vis_miles': json_data['current']['vis_miles'],
                    'uv': json_data['current']['uv'],
                    'gust_mph': json_data['current']['gust_mph'],
                    'gust_kph': json_data['current']['gust_kph'],
                    'air_quality_co': json_data['current']['air_quality']['co'],
                    'air_quality_no2': json_data['current']['air_quality']['no2'],
                    'air_quality_o3': json_data['current']['air_quality']['o3'],
                    'air_quality_so2': json_data['current']['air_quality']['so2'],
                    'air_quality_pm2_5': json_data['current']['air_quality']['pm2_5'],
                    'air_quality_pm10': json_data['current']['air_quality']['pm10'],
                    'air_quality_us_epa_index': json_data['current']['air_quality']['us-epa-index'],
                    'air_quality_gb_defra_index': json_data['current']['air_quality']['gb-defra-index']
                }
                data_list.append(flat_data)

    table = pa.Table.from_pandas(pd.DataFrame(data_list))
    storage_options = {"AWS_ACCESS_KEY_ID": ACCESS_ID,
                       "AWS_SECRET_ACCESS_KEY": ACCESS_KEY, "AWS_S3_ALLOW_UNSAFE_RENAME": "true"}
    
    write_deltalake(f"s3://{bucket}/bronze/",
                    table, mode='append', storage_options=storage_options)


def save_history():
    s3 = boto3.client('s3')
    bucket = 'weather-datalake-bucket'
    destination = 'landing'
    for file in s3.list_objects_v2(Bucket='weather-datalake-bucket', Prefix=destination)['Contents']:
        path = file['Key']
        history_path = path.replace("landing", "history")
        s3.copy_object(
            Bucket=bucket,
            CopySource={'Bucket': bucket, 'Key': path},
            Key=history_path
        )
        s3.delete_object(
            Bucket=bucket,
            Key=path
        )


dag = DAG('etl_pipeline',
          default_args=default_args,
          schedule_interval="0 8 * * *")


collect_data = PythonOperator(
    task_id='collect_data',
    python_callable=fetch_api,
    dag=dag
)

transform_data = PythonOperator(
    task_id='transform_landing_to_bronze',
    python_callable=landing_to_bronze,
    dag=dag
)

copy_to_history = PythonOperator(
    task_id='copy_to_history',
    python_callable=save_history,
    dag=dag
)

collect_data  >> transform_data >> copy_to_history