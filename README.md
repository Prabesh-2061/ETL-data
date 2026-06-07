# Weather ETL Pipeline

A production-style data engineering pipeline that fetches real-time weather data, processes it with Apache Spark, and stores it in a MinIO data lake — all orchestrated by Apache Airflow.

---

## Architecture
Open-Meteo API
│
▼
Airflow (raw_weather asset)
fetches weather hourly for 5 cities
stores raw JSON → PostgreSQL
│
▼
Spark (spark_to_minio asset)
reads raw data from PostgreSQL
parses + cleans JSON payload
engineers features
writes parquet → MinIO data lake

---

## Tech Stack

| Tool | Purpose |
|------|---------|
| Apache Airflow 3.2.1 | Pipeline orchestration |
| Apache Spark 3.5.1 | Distributed data processing |
| PostgreSQL 16 | Raw data storage |
| MinIO AIStor | Data lake (S3-compatible object storage) |
| Redis | Celery task queue |
| Docker Compose | Container orchestration |

---

## Cities Tracked

- Kathmandu, Nepal
- New Delhi, India
- Tokyo, Japan
- Beijing, China
- Paris, France

---

## Data Flow

### Bronze Layer — Raw Data
Airflow fetches weather data from the [Open-Meteo API](https://open-meteo.com/) every hour and stores the raw JSON response in PostgreSQL's `raw_weather` table as-is. No transformation at this stage.

### Silver Layer — Clean Data
Spark reads from PostgreSQL, parses the JSON payload, and flattens it into structured columns:
- `city`
- `temperature`
- `wind_speed`
- `weather_time`

### Gold Layer — Feature Engineered Data
Spark derives two additional features:
- `temp_squared` — temperature squared (useful for ML models)
- `wind_ratio` — wind speed divided by temperature + 1

Final data is written to MinIO as Parquet files at `s3a://weather-lake/features/`.

---

## Project Structure
├── dags/
│   ├── postgre.py          # Airflow assets (raw_weather + spark_to_minio)
│   └── weather_etl.py      # PySpark transformation script
├── spark-jars/             # JAR files (not tracked in git — see setup below)
├── config/                 # Airflow config
├── logs/                   # Airflow logs
├── plugins/                # Airflow plugins
├── Dockerfile              # Custom Airflow image with Spark installed
├── docker-compose.yaml     # All services
├── minio.license           # MinIO AIStor license (not tracked in git)
├── download-jars.sh        # Script to download required JARs
└── .env                    # Environment variables (not tracked in git)

---

## Prerequisites

- Docker Desktop installed and running
- At least 8GB RAM allocated to Docker
- At least 10GB free disk space
- MinIO AIStor license file (`minio.license`)

---

## Setup

### 1 — Clone the repository

```bash
git clone https://github.com/Prabesh-2061/ETL-data.git
cd ETL-data
```

### 2 — Download required JARs

The JAR files are too large for GitHub. Download them with:

```bash
bash download-jars.sh
```

This downloads:
- `postgresql-42.7.3.jar` — PostgreSQL JDBC driver
- `hadoop-aws-3.3.4.jar` — S3A filesystem connector
- `aws-java-sdk-bundle-1.12.262.jar` — AWS SDK for MinIO communication

### 3 — Add your MinIO license

Place your MinIO AIStor license file in the project root:
minio.license

### 4 — Create your `.env` file

```bash
AIRFLOW_UID=50000
FERNET_KEY=your_fernet_key_here
SECRET_KEY=your_secret_key_here
AIRFLOW__API_AUTH__JWT_SECRET=airflow_jwt_secret
AIRFLOW__API_AUTH__JWT_ISSUER=airflow
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow
_PIP_ADDITIONAL_REQUIREMENTS=
```

Generate keys:
```bash
# Fernet key
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

# Secret key
python -c "import secrets; print(secrets.token_hex(32))"
```

### 5 — Build and start

```bash
docker compose build    # takes ~10 minutes (downloads Spark)
docker compose up -d
```

### 6 — Set up MinIO bucket

1. Open `http://localhost:9001`
2. Login with `admin` / `password123`
3. Create a bucket named `weather-lake`

### 7 — Set up Airflow Spark connection

1. Open `http://localhost:8080`
2. Login with `airflow` / `airflow`
3. Go to **Admin → Connections → +**
4. Fill in:
Connection Id:    spark_default
Connection Type:  Spark
Host:             spark://spark-master
Port:             7077

5. Click **Save**

### 8 — Set up Airflow Postgres connection

1. Go to **Admin → Connections → +**
2. Fill in:
Connection Id:    weather_db
Connection Type:  Postgres
Host:             postgres
Port:             5432
Database:         weather_db
Login:            airflow
Password:         airflow

3. Click **Save**

---

## Running the Pipeline

The pipeline runs automatically on a schedule:

- `raw_weather` → every hour
- `spark_to_minio` → triggered automatically after `raw_weather` completes

To trigger manually:
1. Go to `http://localhost:8080`
2. Find `raw_weather` asset
3. Click **Trigger**
4. `spark_to_minio` will run automatically after

---

## Verifying Results

After the pipeline runs, verify data landed in MinIO:

1. Open `http://localhost:9001`
2. Login with `admin` / `password123`
3. Navigate to `weather-lake` → `features/`
4. You should see `.parquet` files

---

## Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow UI | http://localhost:8080 | airflow / airflow |
| Spark Master UI | http://localhost:8081 | — |
| Spark Worker UI | http://localhost:8082 | — |
| MinIO Console | http://localhost:9001 | admin / password123 |
| PostgreSQL | localhost:5432 | airflow / airflow |

---

## Troubleshooting

**JARs not found error**
Run `bash download-jars.sh` and rebuild: `docker compose build`

**MinIO license error**
Make sure `minio.license` is in the project root

**DAG not showing in Airflow**
Wait 30 seconds for the DAG processor to pick it up, then refresh

**Spark job fails**
Check logs in Airflow UI → click the `spark_to_minio` task → Logs

---

## License

Apache License 2.0
