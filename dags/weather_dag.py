from airflow import DAG
from datetime import timedelta, datetime
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import HttpOperator 
import json
from airflow.operators.python import PythonOperator
import pandas as pd
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Define the local file storage path
LOCAL_FILE_PATH = "/usr/local/airflow/include/data_files"

# Define the S3 bucket and key
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
WEATHER_API_KEY = os.getenv("WEATHER_API_KEY")

# Ensure the required environment variables are set
if not WEATHER_API_KEY:
    raise ValueError("Missing environment variable: WEATHER_API_KEY. Set it in .env or Airflow UI.")
if not S3_BUCKET_NAME:
    raise ValueError("Missing environment variable: S3_BUCKET_NAME. Set it in .env or Airflow UI.")

def kelvin_to_fahrenheit(temp_in_kelvin):
    return (temp_in_kelvin - 273.15) * (9/5) + 32

def transform_load_data(task_instance, **kwargs):
    data = task_instance.xcom_pull(task_ids="extract_weather_data")

    city = data["name"]
    weather_description = data["weather"][0]['description']
    temp_fahrenheit = kelvin_to_fahrenheit(data["main"]["temp"])
    feels_like_fahrenheit = kelvin_to_fahrenheit(data["main"]["feels_like"])
    min_temp_fahrenheit = kelvin_to_fahrenheit(data["main"]["temp_min"])
    max_temp_fahrenheit = kelvin_to_fahrenheit(data["main"]["temp_max"])
    pressure = data["main"]["pressure"]
    humidity = data["main"]["humidity"]
    wind_speed = data['wind']["speed"]
    time_of_record = datetime.utcfromtimestamp(data['dt'] + data['timezone'])
    sunrise_time = datetime.utcfromtimestamp(data['sys']['sunrise'] + data['timezone'])
    sunset_time = datetime.utcfromtimestamp(data['sys']['sunset'] + data['timezone'])

    transformed_data = {
        "City": city,
        "Description": weather_description,
        "Temperature (F)": temp_fahrenheit,
        "Feels Like (F)": feels_like_fahrenheit,
        "Minimum Temp (F)": min_temp_fahrenheit,
        "Maximum Temp (F)": max_temp_fahrenheit,
        "Pressure": pressure,
        "Humidity": humidity,
        "Wind Speed": wind_speed,
        "Time of Record": time_of_record,
        "Sunrise (Local Time)": sunrise_time,
        "Sunset (Local Time)": sunset_time     
    }

    df_data = pd.DataFrame([transformed_data])

    # Generate filename with timestamp
    dt_string = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    file_name = f"current_weather_data_kansas_{dt_string}.csv"
    file_path = os.path.join(LOCAL_FILE_PATH, file_name)

    # Save CSV locally
    df_data.to_csv(file_path, index=False)

    # Push file path & filename separately to XCom
    task_instance.xcom_push(key="file_path", value=file_path)
    task_instance.xcom_push(key="file_name", value=file_name)

    return file_path

# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 5),  # Fixed past date to avoid Airflow issues
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    'weather_dag',
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=False,
) as dag:

    is_weather_api_ready = HttpSensor(
        task_id='is_weather_api_ready',
        http_conn_id='weathermap_api',
        endpoint=f'/data/2.5/weather?q=Kansas&appid={WEATHER_API_KEY}',
        poke_interval=5,
        timeout=20
    )

    extract_weather_data = HttpOperator(
        task_id='extract_weather_data',
        http_conn_id='weathermap_api',
        endpoint=f'/data/2.5/weather?q=Kansas&appid={WEATHER_API_KEY}',
        method='GET',
        response_filter=lambda r: json.loads(r.text),
        log_response=True
    )

    transform_load_weather_data = PythonOperator(
        task_id='transform_load_weather_data',
        python_callable=transform_load_data,
        provide_context=True
    )

    upload_to_s3 = LocalFilesystemToS3Operator(
        task_id="upload_to_s3",
        filename="{{ ti.xcom_pull(task_ids='transform_load_weather_data', key='file_path') }}",
        dest_key="weather_data/{{ ti.xcom_pull(task_ids='transform_load_weather_data', key='file_name') }}",
        dest_bucket=S3_BUCKET_NAME,
        aws_conn_id="aws_default",
        replace=True,
    )

    is_weather_api_ready >> extract_weather_data >> transform_load_weather_data >> upload_to_s3