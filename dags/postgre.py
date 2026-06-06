from airflow.providers.postgres.hooks.postgres import PostgresHook
# from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from pendulum import duration
from airflow.sdk import asset, Context, task
import requests
import json
import logging

cities = {
    "New Delhi": {
        "latitude": 28.6139,
        "longitude": 77.2090
    },
    "Tokyo": {
        "latitude": 35.6895,
        "longitude": 139.6917
    },
    "Beijing": {
        "latitude": 39.9042,
        "longitude": 116.4074
    },
    "Paris": {
        "latitude": 48.8566,
        "longitude": 2.3522
    },
    "Kathmandu": {
        "latitude": 27.7172,
        "longitude": 85.3240
    }
}

log = logging.getLogger(__name__)

# =====================================
# FAILURE ALERT
# =====================================
def failure_alert(context: Context):
    task_id = context["task_instance"].task_id
    dag_id = context["task_instance"].dag_id
    execution_time = context["logical_date"]
    error_message = str(context.get("exception"))
    log.error(f"Alert:{dag_id}.{task_id} failed")

    hook = PostgresHook(postgres_conn_id="weather_db")
    conn = hook.get_conn()
    with conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """INSERT INTO pipeline_runs(asset_name, status, execution_time, message)
                   VALUES (%s, %s, %s, %s)
                """,
                (task_id, "Failed", execution_time, error_message)
            )


# 1. RAW WEATHER ASSET
# -------------------------------------------------------------------

@asset(schedule="@hourly", on_failure_callback=failure_alert)
@task(retries=3, retry_delay=duration(minutes=1))
def raw_weather():
    try:
        for city, loc in cities.items():
            lat = loc["latitude"]
            lon = loc["longitude"]
            url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&current_weather=true"

            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            current_data = data.get("current_weather")
            if not current_data:
                raise ValueError("Missing current_weather in API response")

            weather_time = data["current_weather"]["time"]

            hook = PostgresHook(postgres_conn_id="weather_db")
            conn = hook.get_conn()

            with conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        INSERT INTO raw_weather(
                            weather_time,
                            payload,
                            city
                        )
                        VALUES (%s, %s, %s)
                        ON CONFLICT(city, weather_time)
                        DO UPDATE SET
                            payload = EXCLUDED.payload
                        """,
                        (weather_time, json.dumps(data), city)
                    )
    except Exception as e:
        log.error(f"Error fetching or storing weather data: {e}")
        raise

# -------------------------------------------------------------------
# 2. SPARK ETL ASSET
# =====================================
@asset(schedule=raw_weather, name="spark_to_minio", on_failure_callback=failure_alert)
@task(retries=1, retry_delay=duration(minutes=2))
def spark_to_minio(**context):
    from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator 
    try:
        SparkSubmitOperator(
            task_id="spark_etl",
            application="/opt/spark/dags/weather_etl.py",
            conn_id="spark_default",
            jars=   "/opt/airflow/spark-jars/postgresql.jar,"
                    "/opt/airflow/spark-jars/hadoop-aws-3.3.4.jar,"
                    "/opt/airflow/spark-jars/aws-java-sdk-bundle-1.12.262.jar",
            verbose=True,
        ).execute(context=context)             
        log.info("Spark ETL completed successfully — data written to MinIO")
    except Exception as e:
        log.error(f"Spark ETL failed: {e}")
        raise