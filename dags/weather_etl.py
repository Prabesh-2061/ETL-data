from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# =========================================================
# 1. CREATE SPARK SESSION
# =========================================================
spark = SparkSession.builder \
    .appName("WeatherETL") \
    .config("spark.jars", "/opt/airflow/spark-jars/postgresql.jar,"
                          "/opt/airflow/spark-jars/hadoop-aws-3.3.4.jar,"
                          "/opt/airflow/spark-jars/aws-java-sdk-bundle-1.12.262.jar") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

# =========================================================
# 2. POSTGRES → SPARK (RAW INGESTION)
# =========================================================
df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/weather_db") \
    .option("dbtable", "raw_weather") \
    .option("user", "airflow") \
    .option("password", "airflow") \
    .option("driver", "org.postgresql.Driver") \
    .load()

# =========================================================
# 3. DEFINE JSON SCHEMA
# =========================================================
schema = StructType([
    StructField("current_weather", StructType([
        StructField("temperature", DoubleType()),
        StructField("windspeed", DoubleType()),
        StructField("time", StringType())
    ]))
])

# =========================================================
# 4. PARSE JSON PAYLOAD
# =========================================================
df2 = df.withColumn("json", from_json(col("payload"), schema))

# =========================================================
# 5. FLATTEN TO CLEAN DATASET
# =========================================================
clean_df = df2.select(
    col("city"),
    col("json.current_weather.temperature").alias("temperature"),
    col("json.current_weather.windspeed").alias("wind_speed"),
    col("json.current_weather.time").alias("weather_time")
)

# =========================================================
# 6. FEATURE ENGINEERING
# =========================================================
features_df = clean_df.withColumn(
    "temp_squared",
    col("temperature") * col("temperature")
).withColumn(
    "wind_ratio",
    col("wind_speed") / (col("temperature") + 1)
)

# =========================================================
# 7. WRITE TO MINIO (DATA LAKE)
# =========================================================
features_df.write \
    .mode("overwrite") \
    .parquet("s3a://weather-lake/features/")

# =========================================================
# 8. VERIFY
# =========================================================
features_df.show(10, truncate=False)


