from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from pyspark.sql import SparkSession
from datetime import datetime
import requests
import boto3
import json
import os

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
    spark = SparkSession.builder \
        .appName("Transform JSON to Delta") \
        .getOrCreate()

    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket='s3://weather-datalake-bucket/bronze/', Key='data.json')
    data = json.loads(obj['Body'].read().decode('utf-8'))

    df = spark.createDataFrame(data)
    delta_table_path = "s3a://your-bronze-bucket/data"
    df.write.format("delta").mode("overwrite").save(delta_table_path)

dag = DAG('etl_pipeline',
          default_args=default_args,
          schedule_interval="0 8 * * *")


collect_data = PythonOperator(
    task_id='collect_data',
    python_callable=fetch_api,
    dag=dag
)

transform_data = PythonOperator(
    task_id='transform_data',
    python_callable=landing_to_bronze,
    dag=dag
)

collect_data  >> transform_data
