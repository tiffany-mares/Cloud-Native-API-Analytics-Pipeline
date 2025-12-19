# Cloud-Native API Analytics Pipeline

A production-grade data pipeline that ingests data from multiple external APIs, stages it in cloud storage, and loads it into Snowflake for analytics.

## 🏗️ Architecture

```
External APIs → Ingestion Service → Cloud Storage (S3) → Snowpipe → Snowflake → Analytics
```

### Components

| Component | Technology | Purpose |
|-----------|------------|---------|
| Ingestion | Python | Fetch, normalize, and write data files |
| Staging | S3/Azure Blob | Partitioned raw data storage |
| Loading | Snowpipe | Auto-ingest staged files |
| Warehouse | Snowflake | RAW → CLEAN → ANALYTICS layers |
| Orchestration | Apache Airflow | Scheduling, retries, monitoring |
| CI/CD | GitHub Actions | Test, lint, deploy |

## 📁 Project Structure

```
├── dags/                    # Airflow DAG definitions
├── src/
│   ├── auth/               # Authentication (OAuth2, API keys)
│   ├── clients/            # API client wrappers
│   ├── transform/          # Data transformation logic
│   └── utils/              # Logging, file I/O, helpers
├── sql/                    # Snowflake DDL & transformations
├── tests/                  # Unit and integration tests
├── docs/                   # Documentation
├── .github/workflows/      # CI/CD pipelines
├── docker-compose.yml      # Local development environment
└── requirements.txt        # Python dependencies
```

## 🚀 Quick Start

### Prerequisites

- Python 3.11+
- Docker & Docker Compose
- Snowflake account
- AWS account (for S3) or Azure account (for Blob Storage)

### Local Development Setup

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd cloud-native-api-analytics-pipeline
   ```

2. **Create virtual environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -r requirements.txt
   ```

3. **Configure environment variables**
   ```bash
   cp .env.example .env
   # Edit .env with your credentials
   ```

4. **Start local services**
   ```bash
   docker-compose up -d
   ```

5. **Access Airflow UI**
   - URL: http://localhost:8080
   - Username: `admin`
   - Password: `admin`

### Running Tests

```bash
# Run all tests
pytest tests/ -v

# Run with coverage
pytest tests/ -v --cov=src --cov-report=html
```

### Linting & Formatting

```bash
# Check code style
ruff check src/ tests/
black --check src/ tests/

# Auto-fix issues
ruff check --fix src/ tests/
black src/ tests/
```

## 📊 Data Model

### Snowflake Layers

| Layer | Naming | Description |
|-------|--------|-------------|
| **RAW** | `RAW_<SOURCE>_JSON` | Raw VARIANT data with metadata |
| **CLEAN** | `CLN_<ENTITY>` | Typed, deduplicated, standardized |
| **ANALYTICS** | `ANL_<MART>` | Aggregations, KPIs, reports |

### Staging Layout

```
s3://bucket/project/source=<name>/dt=YYYY-MM-DD/hour=HH/part-*.jsonl
```

## 🔐 Security

- **Secrets**: Stored in AWS Secrets Manager / Azure Key Vault
- **RBAC**: Separate roles for loader vs analyst
- **Encryption**: Data encrypted at rest and in transit
- **No credentials in code**: All secrets via environment variables

## 📈 Monitoring & Observability

- **Structured Logging**: JSON-formatted logs for easy parsing
- **Metrics**: Records fetched, files written, rows loaded
- **Alerts**: Task failures, Snowpipe errors, data freshness breaches

## 🛠️ Development

### Adding a New Data Source

1. Create auth handler in `src/auth/`
2. Implement API client in `src/clients/`
3. Add transformation logic in `src/transform/`
4. Create Airflow DAG in `dags/`
5. Add Snowflake DDL in `sql/`

### CI/CD Pipeline

- **PR Checks**: Lint, test, DAG validation
- **Main Branch**: Deploy to staging
- **Tags (v*)**: Deploy to production

## 📚 Documentation

See the `/docs` folder for:
- Architecture diagrams
- Data flow documentation
- Runbooks and troubleshooting guides
- Security model

## 📄 License

[Add your license here]

