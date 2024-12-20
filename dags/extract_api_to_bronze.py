import json
from datetime import datetime, timedelta

import pandas as pd
import pyarrow as pa
import requests
from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from deltalake import write_deltalake
from utils.cities import cities

default_args = {
    'owner': 'Francisco Santos',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 28),
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}


def fetch_api():
    api_key = Variable.get('weather_api_key')
    s3_hook = S3Hook(aws_conn_id='aws_conn')

    for city in cities:
        url = f'http://api.weatherapi.com/v1/current.json?key={api_key}&q={city}&aqi=yes'
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        file_name = f"landing/{city}-{datetime.now().strftime('%d-%m-%Y-%H-%M-%S')}.json"

        s3_hook.load_string(
            string_data=json.dumps(data),
            key=file_name,
            bucket_name='weather-datalake-bucket',
            replace=True,
        )


def landing_to_bronze(bucket, access_id, access_key):
    s3_hook = S3Hook(aws_conn_id='aws_conn')

    data_list = []

    keys = s3_hook.list_keys(bucket_name=bucket, prefix='landing')

    if keys:
        for object_key in keys:
            if object_key.endswith('.json'):
                file_content = s3_hook.read_key(
                    key=object_key, bucket_name=bucket
                )
                json_data = json.loads(file_content)

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

    storage_options = {
        'AWS_ACCESS_KEY_ID': access_id,
        'AWS_SECRET_ACCESS_KEY': access_key,
        'AWS_S3_ALLOW_UNSAFE_RENAME': 'true',
    }

    write_deltalake(
        f's3://{bucket}/bronze/raw_weather_data',
        table,
        mode='append',
        storage_options=storage_options,
    )


def save_history():
    s3_hook = S3Hook(aws_conn_id='aws_conn')
    bucket = 'weather-datalake-bucket'

    keys = s3_hook.list_keys(bucket_name=bucket, prefix='landing')

    if keys:
        for path in keys:
            if path.endswith('.json'):
                history_path = path.replace('landing', 'history')

                s3_hook.get_conn().copy_object(
                    Bucket=bucket,
                    CopySource={'Bucket': bucket, 'Key': path},
                    Key=history_path,
                )

                s3_hook.delete_objects(bucket=bucket, keys=[path])


dag = DAG(
    'extract_api_to_bronze',
    default_args=default_args,
    schedule_interval='0 * * * *',
    catchup=False,
)

start = DummyOperator(
    task_id='start',
    dag=dag
    )

task_save_history = PythonOperator(
    task_id='task_save_history',
    python_callable=save_history,
    dag=dag
)

task_collect_data = PythonOperator(
    task_id='task_collect_data',
    python_callable=fetch_api,
    dag=dag
)

task_landing_to_bronze = PythonOperator(
    task_id='task_landing_to_bronze',
    python_callable=landing_to_bronze,
    dag=dag,
    op_kwargs={
        'bucket': 'weather-datalake-bucket',
        'access_id': Variable.get('aws_access_id'),
        'access_key': Variable.get('aws_access_key'),
    },
)

end = DummyOperator(
    task_id='end',
    dag=dag
    )

(
    start
    >> task_save_history
    >> task_collect_data
    >> task_landing_to_bronze
    >> end
)
