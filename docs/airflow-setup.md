# Airflow Local Setup Guide

## Phase 6: Airflow Orchestration with Docker

### Prerequisites

- Docker Desktop installed and running
- Docker Compose v2+

### Step 22: Start Airflow

#### 1. Create logs directory

```bash
mkdir -p logs
```

#### 2. Set Airflow UID (Linux/Mac)

```bash
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

On Windows, create `.env` with:
```
AIRFLOW_UID=50000
```

#### 3. Initialize Airflow

```bash
docker-compose up airflow-init
```

Wait for the message: "Airflow initialized successfully!"

#### 4. Start Airflow services

```bash
docker-compose up -d
```

#### 5. Access Airflow UI

- **URL**: http://localhost:8080
- **Username**: `admin`
- **Password**: `admin`

### Verify Setup

1. Open http://localhost:8080
2. Login with admin/admin
3. Find `example_verification_dag` in the DAG list
4. Enable the DAG (toggle switch)
5. Trigger the DAG manually (play button)
6. Check task logs to verify imports work

### Docker Commands

```bash
# Start all services
docker-compose up -d

# View logs
docker-compose logs -f airflow-webserver
docker-compose logs -f airflow-scheduler

# Stop all services
docker-compose down

# Rebuild after requirements.txt changes
docker-compose build --no-cache
docker-compose up -d

# Reset everything (including database)
docker-compose down -v
docker-compose up airflow-init
docker-compose up -d
```

### Mounted Volumes

| Local Path | Container Path | Purpose |
|------------|----------------|---------|
| `./dags/` | `/opt/airflow/dags/` | DAG files |
| `./src/` | `/opt/airflow/src/` | Python source code |
| `./logs/` | `/opt/airflow/logs/` | Airflow logs |
| `./sql/` | `/opt/airflow/sql/` | SQL scripts |

### Environment Variables

Add to your `.env` file:

```bash
AIRFLOW_UID=50000

# Snowflake
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password

# AWS
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
S3_BUCKET=your_bucket

# APIs
API_A_BASE_URL=https://api.example.com
API_A_OAUTH_CLIENT_ID=your_client_id
# ... etc
```

### Troubleshooting

**DAGs not appearing?**
- Check scheduler logs: `docker-compose logs airflow-scheduler`
- Verify DAG syntax: `docker-compose exec airflow-webserver airflow dags list`

**Import errors?**
- Ensure `src/` has `__init__.py` files
- Check PYTHONPATH is set correctly
- Rebuild: `docker-compose build --no-cache`

**Database errors?**
- Reset: `docker-compose down -v && docker-compose up airflow-init`

### LocalStack (Optional S3 Mock)

For local S3 testing without AWS:

```bash
# Start with LocalStack profile
docker-compose --profile local-s3 up -d

# S3 endpoint: http://localhost:4566
# Create bucket:
aws --endpoint-url=http://localhost:4566 s3 mb s3://test-bucket
```

Set in `.env`:
```
S3_BUCKET=test-bucket
AWS_ENDPOINT_URL=http://localhost:4566
```

