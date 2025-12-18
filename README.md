# ICS474 – Real-Time IoT Big Data Pipeline

An end-to-end real-time Big Data analytics pipeline built for the **ICS474 – Big Data Analytics** course.  
The system ingests simulated IoT telemetry data, streams it through Apache Kafka, processes it using Spark Structured Streaming, stores results in PostgreSQL, visualizes insights via Streamlit, and includes a smart alerting layer for anomaly detection.

## System Architecture

The pipeline follows a real-time streaming architecture:

Producer (Python – simulated IoT data)
→ Apache Kafka (message broker)
→ Spark Structured Streaming (processing + validation + smart rules)
→ PostgreSQL (storage layer)
→ Streamlit Dashboard (visualization + monitoring)

## Technologies Used

- Apache Kafka – real-time messaging and buffering
- Apache Spark (PySpark Structured Streaming) – stream processing and analytics
- PostgreSQL – persistent storage layer
- Docker & Docker Compose – containerized deployment
- Python – producer, Spark job logic, and dashboard
- Streamlit – real-time visualization dashboard

## Features

- Simulated real-time IoT telemetry ingestion
- Kafka-based streaming data pipeline
- Schema validation and corrupt record handling
- Real-time Spark Structured Streaming processing
- Persistent storage of valid and invalid records
- Live dashboard with auto-refresh metrics and charts
- Smart alerting layer for anomaly detection (overspeed, overheat, low fuel)

## Project Structure

ics474-iot-pipeline/
├── docker/ # Docker Compose (Kafka, Spark, Postgres)
├── producer/ # Python Kafka producer (simulated IoT data)
├── spark_job/ # Spark Structured Streaming job
├── db/ # Database schemas
├── dashboard/ # Streamlit visualization dashboard
├── README.md
├── CREDITS.md

````bash
## How to Run the Project


### 1. Start infrastructure
```bash
cd docker
docker compose up -d

### 2. Create database schema
docker exec -i postgres psql -U iot_user -d iot_db < db/schema.sql

### 3. Create Kafka topic
docker exec -it kafka kafka-topics --create \
  --topic raw_telemetry \
  --bootstrap-server kafka:29092 \
  --partitions 1 \
  --replication-factor 1

### 4. Start Spark streaming job
docker exec -it spark /opt/spark/bin/spark-submit \
  --master "local[*]" \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7,org.postgresql:postgresql:42.7.5 \
  /opt/spark-apps/streaming_job.py

### 5. Start the producer
cd producer
source .venv/bin/activate
python producer.py

### 6. Start the dashboard
cd dashboard
source .venv/bin/activate
streamlit run app.py

---
````

## 7. Smart layer explanation (bonus marks)

## Smart Layer (Anomaly Detection)

A smart analytics layer was implemented using Spark Structured Streaming rules.
The system detects anomalies such as:

- Overspeed events
- Engine overheating
- Low fuel levels
- Invalid GPS coordinates

Detected anomalies are stored in a dedicated PostgreSQL alerts table and visualized in real time on the dashboard.

## Course Information

- Course: ICS474 – Big Data Analytics
- Institution: King Fahd University of Petroleum & Minerals
- Project Type: End-to-End Big Data Pipeline
- Option: Rebuild an existing project with extensions
