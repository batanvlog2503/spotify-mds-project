Analyze the entire repository and generate a world-class README.md similar to top open-source projects on GitHub.



Requirements:

- Understand the codebase before writing.

- Infer architecture from source code.

- Create professional documentation.

- Include badges.

- Include architecture overview.

- Include setup instructions.

- Include environment variables table.

- Include API documentation.

- Include folder structure tree.

- Include development workflow.

- Include deployment guide.

- Include troubleshooting section.

- Include FAQ section.

- Include contribution guide.

- Use clean markdown formatting.



Output ONLY the final README.md content.

Exploring the repository structure and key files to understand the architecture before writing the README.

Spotify MDS — Real-Time Streaming Data Pipeline
Python Apache Kafka Apache Airflow dbt Snowflake MinIO Docker License

End-to-end Medallion Architecture data pipeline that simulates Spotify listening events, streams them through Kafka, lands raw data in a MinIO data lake, orchestrates batch loads into Snowflake with Airflow, and transforms data into analytics-ready Gold tables with dbt.

Table of Contents
Overview
Architecture
Tech Stack
Prerequisites
Quick Start
Environment Variables
API & Interface Documentation
Folder Structure
Development Workflow
Deployment Guide
Troubleshooting
FAQ
Contributing
License
Overview
This project demonstrates a production-style real-time → batch → warehouse data engineering workflow:

Ingest — A Python simulator generates realistic Spotify-like events and publishes them to Kafka.
Buffer — Kafka decouples producers from consumers and absorbs traffic spikes.
Land (Bronze) — A Kafka consumer batches events and writes NDJSON files to MinIO with date/hour partitioning.
Orchestrate — Apache Airflow runs an hourly DAG to extract files from MinIO and load raw records into Snowflake.
Transform (Silver & Gold) — dbt models clean, validate, and aggregate data for analytics.
Data Flow
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  Simulator  │────▶│    Kafka    │────▶│  Consumer   │────▶│    MinIO    │
│  (Producer) │     │   (Broker)  │     │ (Kafka→S3)  │     │   (Bronze)  │
└─────────────┘     └─────────────┘     └─────────────┘     └──────┬──────┘
                                                                    │
                                                                    ▼
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  dbt Gold   │◀────│ dbt Silver  │◀────│  Snowflake  │◀────│   Airflow   │
│ (Analytics) │     │  (Cleaned)  │     │   (Bronze)  │     │    (DAG)    │
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
Why This Architecture?
Layer	Purpose
Kafka
Absorbs high-throughput event streams without overloading downstream systems
MinIO
Cheap, durable object storage; single source of truth for raw JSON
Airflow
Schedules batch loads; minimizes Snowflake warehouse uptime and cost
Snowflake
Columnar warehouse optimized for analytics and SQL transformations
dbt
Version-controlled, testable SQL transformations (Bronze → Silver → Gold)
Architecture
Medallion Layers
Layer	Storage	Format	Description
Bronze
MinIO + Snowflake
NDJSON files / raw table
Immutable raw events, minimal processing
Silver
Snowflake (spotify_silver)
View/Table
Null filtering, timestamp parsing, deduplication-ready
Gold
Snowflake (top_songs, user_engagement)
Tables
Business metrics for dashboards and reporting
Docker Services
                    Docker Compose
                          │
    ┌─────────────────────┼─────────────────────┐
    │                     │                     │
    ▼                     ▼                     ▼
┌──────────┐        ┌──────────┐         ┌──────────┐
│Zookeeper │───────▶│  Kafka   │────────▶│ Kafdrop  │
│  :2181   │        │  :29092  │         │  :9000   │
└──────────┘        └──────────┘         └──────────┘
┌──────────┐        ┌──────────┐         ┌──────────┐
│PostgreSQL│───────▶│ Airflow  │────────▶│  MinIO   │
│  :5432   │        │  :8080   │         │:9001/9002│
└──────────┘        └──────────┘         └──────────┘
Airflow DAG: spotify_minio_to_snowflake_bronze
Property	Value
Schedule
@hourly
Retries
1 (5-minute delay)
Catchup
Disabled
extract_data  ──▶  load_raw_to_snow_flake
     │                      │
     ▼                      ▼
List & merge          INSERT into
MinIO JSON files      SPOTIFY_EVENTS_BRONZE
Tech Stack
Category	Technology
Language
Python 3.10+
Streaming
Apache Kafka 7.4.1, Confluent Zookeeper
Object Storage
MinIO (S3-compatible)
Orchestration
Apache Airflow 2.9.3
Warehouse
Snowflake
Transformation
dbt Core + dbt-snowflake
Libraries
kafka-python, boto3, snowflake-connector-python, python-dotenv, Faker
Observability
Kafdrop (Kafka UI), Airflow Web UI, MinIO Console
Prerequisites
Docker Desktop (or Docker Engine + Docker Compose v2)
Python 3.10+ with pip
Snowflake account with a warehouse, database, and schema
Git
At least 8 GB RAM recommended for running the full Docker stack
Quick Start
1. Clone the repository
git clone https://github.com/<your-org>/spotify-mds-project.git
cd spotify-mds-project
2. Configure environment files
Copy and edit the environment templates for each component:

# Docker infrastructure (MinIO, Postgres, Airflow)
cp docker/.env.example docker/.env
# Airflow DAG credentials (MinIO + Snowflake)
cp docker/dags/.env.example docker/dags/.env
# Kafka producer simulator
cp simulator/.env.example simulator/.env
# Kafka → MinIO consumer
cp consumer/.env.example consumer/.env
Security note: Never commit .env files. Use placeholder values in examples and store real credentials in a secrets manager for production.

3. Start infrastructure
cd docker
docker compose up -d
Wait until all services are healthy, then verify:

Service	URL
Airflow UI
http://localhost:8080
Kafdrop (Kafka UI)
http://localhost:9000
MinIO Console
http://localhost:9001
MinIO S3 API
http://localhost:9002
4. Install Python dependencies
cd ..
python -m venv .venv
# Windows
.venv\Scripts\activate
# macOS / Linux
source .venv/bin/activate
pip install -r requirements.txt
pip install Faker
5. Start the event producer
cd simulator
python producer.py
You should see events like:

🎧 Starting Spotify data simulator...
Produced event: play - Blinding Lights by The Weekend (user ...)
6. Start the Kafka consumer
In a second terminal:

cd consumer
python kafka-to-minio.py
Confirm uploads to MinIO:

✅ Uploaded 10 events to MinIO: bronze/date=2026-06-06/hour=14/spotify_events_....json
7. Trigger the Airflow DAG
Open http://localhost:8080
Log in with your AIRFLOW_ADMIN_* credentials
Enable DAG spotify_minio_to_snowflake_bronze
Trigger a manual run or wait for the hourly schedule
8. Run dbt transformations
Configure your dbt profile (~/.dbt/profiles.yml) for Snowflake, then:

cd spotify_dbt/Phamtan2503
dbt debug
dbt run
dbt test
Environment Variables
docker/.env — Infrastructure
Variable	Required	Default	Description
MINIO_ROOT_USER
Yes
—
MinIO admin username
MINIO_ROOT_PASSWORD
Yes
—
MinIO admin password
POSTGRES_USER
Yes
airflow
Airflow metadata DB user
POSTGRES_PASSWORD
Yes
—
Airflow metadata DB password
POSTGRES_DB
Yes
airflow
Airflow metadata database name
AIRFLOW_ADMIN_USER
Yes
—
Airflow web UI username
AIRFLOW_ADMIN_PASSWORD
Yes
—
Airflow web UI password
AIRFLOW_ADMIN_FIRSTNAME
No
—
Airflow admin first name
AIRFLOW_ADMIN_LASTNAME
No
—
Airflow admin last name
AIRFLOW_ADMIN_EMAIL
No
—
Airflow admin email
docker/dags/.env — Airflow DAG (MinIO + Snowflake)
Variable	Required	Default	Description
MINIO_ENDPOINT
Yes
http://minio:9000
MinIO endpoint (Docker network)
MINIO_ACCESS_KEY
Yes
—
MinIO access key
MINIO_SECRET_KEY
Yes
—
MinIO secret key
MINIO_BUCKET
Yes
spotify
S3 bucket name
MINIO_PREFIX
Yes
bronze/date=
Object key prefix filter
SNOWFLAKE_USER
Yes
—
Snowflake username
SNOWFLAKE_PASSWORD
Yes
—
Snowflake password
SNOWFLAKE_ACCOUNT
Yes
—
Account locator (e.g. xy12345.ap-southeast-1)
SNOWFLAKE_WAREHOUSE
Yes
—
Compute warehouse name
SNOWFLAKE_DATABASE
Yes
SPOTIFY_DB
Target database
SNOWFLAKE_SCHEMA
Yes
BRONZE
Target schema
SNOWFLAKE_TABLE
Yes
SPOTIFY_EVENTS_BRONZE
Bronze table name
LOCAL_TEMP_PATH
No
/tmp/spotify_raw.json
Temp file path inside Airflow container
simulator/.env — Event Producer
Variable	Required	Default	Description
KAFKA_BOOTSTRAP_SERVERS
Yes
localhost:29092
Kafka broker address (host)
KAFKA_TOPIC
No
spotify-events
Kafka topic name
USER_COUNT
No
20
Number of simulated users
EVENT_INTERVAL_SECONDS
No
1
Seconds between events
consumer/.env — Kafka → MinIO Consumer
Variable	Required	Default	Description
MINIO_ENDPOINT
Yes
http://localhost:9002
MinIO S3 API (host-mapped port)
MINIO_ACCESS_KEY
Yes
—
MinIO access key
MINIO_SECRET_KEY
Yes
—
MinIO secret key
MINIO_BUCKET
Yes
spotify
Target bucket
KAFKA_TOPIC
Yes
spotify-events
Kafka topic to consume
KAFKA_BOOTSTRAP_SERVER
Yes
localhost:29092
Kafka broker address
KAFKA_GROUP_ID
Yes
spotify-consumer-group
Consumer group for offset tracking
BATCH_SIZE
No
10
Events per MinIO file upload
API & Interface Documentation
This project is an event-driven data pipeline, not a REST API service. The interfaces below define how components communicate.

Event Schema (Kafka Message)
Topic: spotify-events
Format: JSON (UTF-8 serialized)

{
  "event_id": "550e8400-e29b-41d4-a716-446655440000",
  "user_id": "7c9e6679-7425-40de-944b-e07fc1f90ae7",
  "song_id": "a3bb189e-8bf9-3888-9912-ace4e6543002",
  "artist_name": "The Weekend",
  "song_name": "Blinding Lights",
  "event_type": "play",
  "device_type": "mobile",
  "country": "US",
  "timestamp": "2026-06-06T14:30:00.123456Z"
}
Field	Type	Description
event_id
string (UUID v4)
Unique event identifier
user_id
string (UUID v4)
Simulated user identifier
song_id
string (UUID v5)
Deterministic song ID from artist::song
artist_name
string
Artist name
song_name
string
Track title
event_type
enum
play, pause, skip, add_to_playlist
device_type
enum
mobile, desktop, web
country
string
ISO-like country code (US, UK, CA, etc.)
timestamp
string (ISO 8601 UTC)
Event occurrence time
MinIO Object Layout (Bronze)
Bucket: spotify
Path pattern:

bronze/date={YYYY-MM-DD}/hour={HH}/spotify_events_{YYYY-MM-DDTHH-MM-SS}.json
File format: NDJSON (one JSON object per line)

Snowflake Bronze Table
Table: SPOTIFY_DB.BRONZE.SPOTIFY_EVENTS_BRONZE

Column	Type	Description
event_id
STRING
Event UUID
user_id
STRING
User UUID
song_id
STRING
Song UUID
artist_name
STRING
Artist
song_name
STRING
Track
event_type
STRING
Event action
device_type
STRING
Client device
country
STRING
Country code
timestamp
STRING
Raw timestamp string
dbt Models
Model	Layer	Description
spotify_silver
Silver
Filters nulls; parses timestamp → event_ts via TRY_TO_TIMESTAMP_TZ
top_songs
Gold
total_plays and total_skips per song, ordered by plays
user_engagement
Gold
Daily plays, skips, and playlist adds per user/device/country
Service Endpoints
Service	Endpoint	Purpose
Kafka (host)
localhost:29092
Produce/consume from host machine
Kafka (Docker)
kafka:9092
Inter-container communication
Kafdrop
http://localhost:9000
Browse topics, messages, consumer groups
MinIO Console
http://localhost:9001
Bucket and object management
MinIO S3 API
http://localhost:9002
Programmatic S3 access
Airflow
http://localhost:8080
DAG monitoring and manual triggers
PostgreSQL
localhost:5432
Airflow metadata (internal)
Folder Structure
spotify-mds-project/
├── consumer/
│   ├── kafka-to-minio.py       # Kafka consumer → MinIO bronze writer
│   └── .env                    # Consumer configuration (gitignored)
│
├── simulator/
│   ├── producer.py             # Spotify event simulator → Kafka
│   └── .env                    # Producer configuration (gitignored)
│
├── docker/
│   ├── docker-compose.yml      # Full infrastructure stack
│   ├── .env                    # Docker service credentials
│   ├── kafka/
│   │   └── kafka.properties    # Kafka broker configuration
│   ├── dags/
│   │   ├── minio-to-snowflake.py  # Airflow DAG (Bronze ETL)
│   │   └── .env                # DAG runtime secrets
│   ├── logs/                   # Airflow task logs (generated)
│   └── plugins/                # Airflow plugins (optional)
│
├── spotify_dbt/
│   └── Phamtan2503/
│       ├── dbt_project.yml     # dbt project configuration
│       ├── models/
│       │   ├── sources.yml     # Bronze source definitions
│       │   ├── silver/
│       │   │   └── spotify_silver.sql
│       │   ├── gold/
│       │   │   ├── top_songs.sql
│       │   │   └── user_engagement.sql
│       │   └── example/        # dbt starter models
│       └── target/             # dbt compiled artifacts (generated)
│
├── requirements.txt            # Python dependencies
└── README.md                   # This file
Development Workflow
Local development loop
# Terminal 1 — Infrastructure
cd docker && docker compose up -d
# Terminal 2 — Producer
cd simulator && python producer.py
# Terminal 3 — Consumer
cd consumer && python kafka-to-minio.py
# Terminal 4 — dbt (after Airflow loads Snowflake)
cd spotify_dbt/Phamtan2503 && dbt run
Recommended practices
Branch naming: feature/<name>, fix/<name>, docs/<name>
Commit style: Conventional Commits (feat:, fix:, docs:, chore:)
DAG changes: Edit docker/dags/minio-to-snowflake.py; Airflow auto-reloads DAGs
dbt changes: Run dbt run --select <model> for incremental testing
Logs: Check docker/logs/ for Airflow task failures
Testing checklist

 Producer prints events without Kafka connection errors

 Kafdrop shows messages on spotify-events

 Consumer uploads NDJSON files to bronze/ in MinIO

 Airflow DAG extract_data and load_raw_to_snow_flake succeed

 Snowflake bronze table row count increases

 dbt run completes; silver and gold models are queryable
Code style
Python: PEP 8, type hints where practical
SQL: Lowercase keywords, explicit column lists in production models
Environment: All secrets via .env; never hardcode credentials
Deployment Guide
Local / Development
Use the Quick Start instructions. All services run via Docker Compose on a single machine.

Staging / Production Considerations
Component	Local	Production Recommendation
Kafka
Single broker
Managed Kafka (Confluent Cloud, MSK) with replication
MinIO
Single node
AWS S3, GCS, or distributed MinIO
Airflow
Docker Compose
Astronomer, MWAA, or self-hosted K8s
Snowflake
Trial/dev account
Production warehouse with RBAC and masking
Secrets
.env files
Vault, AWS Secrets Manager, or Airflow Connections
Production deployment steps
Provision cloud resources — Snowflake database/schemas, S3 bucket, managed Kafka
Migrate configuration — Replace .env values with secret manager references
Deploy Airflow — Mount DAGs; install boto3, snowflake-connector-python, python-dotenv in the Airflow image
Run consumer as a service — systemd, Kubernetes Deployment, or ECS task with restart policy
Schedule dbt — Airflow BashOperator / DbtCloudRunJobOperator, or CI/CD on merge to main
Enable monitoring — Airflow alerts, Snowflake query history, Kafka consumer lag metrics
Example: Custom Airflow Docker image
FROM apache/airflow:2.9.3
RUN pip install boto3 snowflake-connector-python python-dotenv
CI/CD (recommended)
# .github/workflows/dbt.yml
name: dbt CI
on: [pull_request]
jobs:
  dbt:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: pip install dbt-core dbt-snowflake
      - run: cd spotify_dbt/Phamtan2503 && dbt deps && dbt run && dbt test
        env:
          SNOWFLAKE_ACCOUNT: ${{ secrets.SNOWFLAKE_ACCOUNT }}
          # ... additional secrets
Troubleshooting
Kafka connection refused
Symptom: NoBrokersAvailable or connection timeout

Fix:

Ensure Docker stack is running: docker compose ps
Use localhost:29092 from the host (not 9092)
Inside Docker containers, use kafka:9092
Consumer uploads nothing
Symptom: Consumer runs but no MinIO files appear

Fix:

Confirm producer is running and Kafdrop shows messages
Verify KAFKA_BOOTSTRAP_SERVER (singular) in consumer/.env
Check KAFKA_GROUP_ID — a committed offset may skip old messages; use a new group ID or reset offsets
Lower BATCH_SIZE for faster testing
Airflow DAG fails on extract_data
Symptom: NoSuchBucket or empty file list

Fix:

Confirm MinIO bucket spotify exists and contains bronze/ objects
Verify MINIO_ENDPOINT=http://minio:9000 in docker/dags/.env (Docker network hostname)
Check MINIO_PREFIX matches your partition layout (bronze/date=)
Snowflake load fails
Symptom: Authentication or warehouse errors in load_raw_to_snow_flake

Fix:

Run dbt debug to validate Snowflake credentials
Confirm warehouse is not suspended
Verify SNOWFLAKE_ACCOUNT format: <locator>.<region> (no https://)
Ensure network policy allows Airflow host IP
ModuleNotFoundError: faker
Fix:

pip install Faker
MinIO credential mismatch
Symptom: PartialCredentialsError or AccessDenied

Fix:

Align MINIO_ACCESS_KEY / MINIO_SECRET_KEY with MINIO_ROOT_USER / MINIO_ROOT_PASSWORD
Remove spaces around = in .env files (e.g. use KEY=value, not KEY = value)
dbt source not found
Symptom: Database Error: Object 'SPOTIFY_EVENTS_BRONZE' does not exist

Fix:

Run the Airflow DAG first to populate the bronze table
Confirm sources.yml database/schema/table names match Snowflake exactly
FAQ
Q: Why Kafka instead of writing directly to Snowflake?
A: Kafka buffers high-throughput streams. Snowflake is optimized for batch analytics, not millions of single-row inserts per second. Kafka + MinIO decouple ingestion from warehouse loading and reduce cost.

Q: Why batch events before writing to MinIO?
A: Batching (BATCH_SIZE=10) avoids the small-file problem — too many tiny objects degrade read performance and increase storage overhead.

Q: What is the difference between Bronze in MinIO and Bronze in Snowflake?
A: MinIO holds immutable raw NDJSON files (data lake). Snowflake holds the same data in a queryable table for SQL transformations. Airflow bridges the two.

Q: Can I use AWS S3 instead of MinIO?
A: Yes. Replace MINIO_ENDPOINT with your S3 endpoint (or omit for default AWS), and use IAM credentials. The boto3 client works with any S3-compatible API.

Q: How do I increase event volume for load testing?
A: Lower EVENT_INTERVAL_SECONDS in simulator/.env (e.g. 0.1) and increase USER_COUNT.

Q: Does this use the real Spotify API?
A: No. Events are synthetically generated by producer.py using Faker and a fixed catalog of songs/artists.

Q: How often does data reach Snowflake?
A: The Airflow DAG runs hourly (@hourly). Trigger manually in the Airflow UI for immediate loads.

Contributing
Contributions are welcome. Please follow these steps:

Fork the repository
Create a branch — git checkout -b feature/my-improvement
Make changes — keep diffs focused; update docs if behavior changes
Test locally — run the full pipeline checklist in Development Workflow
Commit — use Conventional Commits
Open a Pull Request — describe what changed, why, and how you tested it
Ideas for contributions
Add dbt tests (unique, not_null, accepted_values on event_type)
Materialize silver/gold models with explicit schema configs
Add Prometheus/Grafana monitoring for Kafka lag
Replace row-by-row Snowflake inserts with COPY INTO / staging
Add docker compose health checks for Airflow services
Create .env.example template files
Code of Conduct
Be respectful, constructive, and collaborative. Harassment and spam will not be tolerated.

License