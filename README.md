<div align="center">

# ✈️ Flight Data Warehouse Project

[![Docker](https://img.shields.io/badge/Docker-20.10%2B-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169E1?style=for-the-badge&logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.0%2B-017CEE?style=for-the-badge&logo=apache-airflow&logoColor=white)](https://airflow.apache.org/)
[![Python](https://img.shields.io/badge/Python-3.8%2B-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![Talend](https://img.shields.io/badge/Talend-8.0.1-FF5B00?style=for-the-badge&logo=talend&logoColor=white)](https://www.talend.com/)

**A comprehensive data engineering project implementing a modern data warehouse pipeline for flight analytics.**

[Quick Start](#🚀-quick-start) • [Architecture](#📊-architecture) • [Data-Model](#🗄️-data-model) • [Performance](#⚡-performance-optimizations)

</div>

---

## 🚀 Project Overview

This project demonstrates an **end-to-end ETL pipeline** processing raw flight and booking data through **bronze**, **silver**, and **gold** layers, culminating in an analytical **Star Schema** for business intelligence and reporting. Data enrichment is performed by integrating a **Weather API**.

## 📊 Architecture

### Data Flow

```mermaid
graph TB

%% ============================
%%         SOURCE LAYER
%% ============================
subgraph Source["Source"]
    Raw[(Local Files/APIs<br/>Flight/Booking Data)]
    WeatherAPI[(External Weather API<br/>Real-time Data)]
end


%% ============================
%%      ETL / PROCESSING
%% ============================
subgraph Processing["ETL / Processing"]
    Python[Python ETL Script<br/>Multi-threaded Ingestion]
    Talend[Talend 8.0.1<br/>Data Transformation/Cleansing]
    Spark[Apache Spark 3.3.0<br/>Batch Processing]
    Airflow[Apache Airflow 2.0+<br/>Workflow Orchestrator]
end


%% ============================
%%         STORAGE LAYER
%% ============================
subgraph Storage["Storage Layers"]
    DBeaver[(PostgreSQL DB<br/>Docker Container)]
    Bronze[Bronze Layer<br/>Raw Data]
    Silver[Silver Layer<br/>Cleaned/Staging Data]
    Gold[Gold Layer<br/>Star Schema]
    Hadoop[(HDFS/Hive<br/>Data Lake)]
end


%% ============================
%%          DATA FLOW
%% ============================

%% Extraction
Raw -->|Extract| Python

%% Load to PostgreSQL
Python -->|Load| DBeaver

%% Bronze to Talend Flow
DBeaver -. Bronze Layer .-> Bronze
Bronze --> Talend

%% Talend to Silver
Talend -->|Transform & Load| Silver

%% Silver Model Processing
Silver -->|Dimensional Model| Talend

%% Talend Weather Enrichment
Talend -->|Enrichment| WeatherAPI

%% Talend to Gold
Talend -->|Load| Gold

%% Talend to Spark
Talend -->|Cleaned Data| Spark

%% Spark to Hadoop
Spark -->|Load| Hadoop


%% ============================
%%      AIRFLOW ORCHESTRATION
%% ============================
Airflow -. orchestrates .-> Python
Airflow -. orchestrates .-> Talend
Airflow -. orchestrates .-> Spark


%% ============================
%%          STYLING
%% ============================
style DBeaver fill:#4169E1,stroke:#4169E1,color:#fff,stroke-width:2px
style Python fill:#3776AB,stroke:#3776AB,color:#fff,stroke-width:2px
style Talend fill:#FF5B00,stroke:#FF5B00,color:#fff,stroke-width:2px
style Spark fill:#E25A1C,stroke:#E25A1C,color:#fff,stroke-width:2px
style Airflow fill:#017CEE,stroke:#017CEE,color:#fff,stroke-width:2px
style Hadoop fill:#66CCFF,stroke:#66CCFF,color:#000,stroke-width:2px
style Bronze fill:#8D6E63,stroke:#5D4037,color:#fff,stroke-width:2px
style Silver fill:#B0BEC5,stroke:#546E7A,color:#000,stroke-width:2px
style Gold fill:#FDD835,stroke:#F9A825,color:#000,stroke-width:2px

```

### Technology Stack

- **Database & Procedures**: PostgreSQL with DBeaver
- **Data Ingestion**: Python (multi-threading, yield buffers)
- **Data Transformation**: Talend 8.0.1 (pagination)
- **Orchestration**: Apache Airflow
- **Data Modeling**: Star Schema
- **Data Enrichment**: Weather API

## 📊 Data Model

### Dimensions

- `DIM_PASSENGER` - Passenger demographics and loyalty status
- `DIM_FLIGHT` - Flight details and status (6M+ records)
- `DIM_AIRPORT` - Airport geographical information
- `DIM_DATE` - Time intelligence for analysis
- `DIM_TICKET` - Booking class and fare information
- `DIM_PAYMENT` - Payment methods and status
- `DIM_WEATHER` - Weather conditions at flight time (API-enriched)

### Fact Table

- `FACT_BOOKING` - Core business metrics (500K+ records)

![Architecture Diagram](images/tables.png)

---

## 📦 Data Sources & Volume

The pipeline ingests data from two primary sources: a large historical file for flight details and a stream of booking records, enriched with external real-time weather data.

| Data Source      | Volume/Type               | Description                                                    | Format  |
| ---------------- | ------------------------- | -------------------------------------------------------------- | ------- |
| **Flight Data**  | 6M+ Historical Records    | Static dataset for `DIM_FLIGHT` and `DIM_AIRPORT`              | CSV     |
| **Booking Data** | 500K+ Incremental Records | Core transaction data for `FACT_BOOKING` and `DIM_PASSENGER`   | CSV/API |
| **Weather API**  | Real-time                 | Enriched weather conditions linked to flight departure/arrival | JSON    |

---

## ⚡ Performance Optimizations

### Python ETL

- **Multi-threading** for parallel data processing
- **Yield with buffers** for memory-efficient chunk processing
- **Batch processing** of 6M+ records in DIM_FLIGHT

### Talend Transformation

- **Pagination implementation** for large dataset handling
- **Efficient data cleansing** before Silver/Gold layers
- **Direct loading** to both Silver and Gold layers

- **Talend Parent Job Zip** : https://drive.google.com/file/d/1nBHI3AoUCiO2dknfszf8PRDOlRK9x1Pk/view?usp=sharing

### Airflow Orchestration

- **Daily scheduled workflows**
- **Dependency management** between components
- **Error handling and monitoring**

## 🛠️ Installation & Setup

# Required

- Python 3.8+
- Apache Airflow 2.0+
- Talend 8.0.1 (Desktop Application)
- Docker Engine 20.10+ (for PostgreSQL DB)
- DBeaver (or any PostgreSQL client)

### Access Web UIs

| Service              | URL                   | Credentials     |
| -------------------- | --------------------- | --------------- |
| **Airflow**          | http://localhost:8082 | airfow / airfow |
| **Spark Master**     | http://localhost:8080 | -               |
| **HDFS NameNode**    | http://localhost:9870 | -               |
| **Jupyter Notebook** | http://localhost:8888 | -               |

## 📁 File Structure

```
ETL-FLIGHT/
├── airflow
│   ├── config
│   ├── dags
│   ├── logs
│   └── plugins
├── config
├── dags
├── data
├── docker-compose.env
├── docker-compose.yml
├── Dockerfile
├── Dockerfile.kafka-connect
├── drivers
│   └── postgresql-42.7.7.jar
├── etl-flight
│   ├── airlines_modified.csv
│   ├── airport.csv
│   ├── api.py
│   ├── bronze_layer_workflows.py
│   ├── common.py
│   ├── constants.py
│   ├── flights_airlines.csv
│   ├── flights_modified.csv
│   ├── jobInfo.properties
│   ├── lib
│   ├── main.py
│   ├── parent_job
│   ├── __pycache__
│   └── requirements.txt
├── etl_scripts
├── flights_airlines.csv
├── flights_modified.csv
├── images
│   ├── architecture.png
│   └── tables.png
├── jobInfo.properties
├── kafka_scripts
│   ├── hsperfdata_root
│   ├── kafka_consumer.py
│   ├── kafka_consumer_stream.py
│   ├── kafka_streaming_job.py
│   ├── spark-5abf7a35-4957-412d-b3a2-30ff589f0315
│   ├── spark-659f5745-06a0-4fdd-b0c4-38537ee2a0b9
├── logs
│   └── scheduler
├── myenv
│   ├── bin
│   ├── include
│   ├── lib
│   ├── lib64 -> lib
│   └── pyvenv.cfg
├── notebooks
│   ├── producer.py
│   ├── sample_spark.ipynb
│   └── spark_local_temp
├── parent_job
│   ├── airport_dim_0_1.jar
│   ├── booking_fact_0_1.jar
│   ├── date_dim_0_1.jar
│   ├── depi
│   ├── flight_dim_0_1.jar
│   ├── items
│   ├── log4j2.xml
│   ├── parent_job_0_1.jar
│   ├── parent_job_run.bat
│   ├── parent_job_run.ps1
│   ├── parent_job_run.sh
│   ├── passenger_dim_0_1.jar
│   ├── payment_dim_0_1.jar
│   ├── src
│   ├── ticket_dim_0_1.jar
│   ├── weather_dim_0_1.jar
│   └── xmlMappings
├── parent_job_localhost.zip
├── parent_job_worker.zip
├── README.md
├── scripts
│   ├── hadoop
│   └── pyspark
├── spark_drivers
├── talend_jobs
```

---

## 🔄 Airflow DAG: ETL-Flight

The main DAG orchestrates the complete ETL pipeline with 12 tasks:

```mermaid
flowchart TB

    %% Outer box: Airflow orchestrates everything
    subgraph AIRFLOW

        %% Bronze layer
        subgraph Bronze
            KAG[Kaggle_file] --> PY[python_etl_load_to_bronze]
            PY --> BRZ_PROC[bronze_procedure]
        end

        %% Silver layer
        subgraph Silver
            BRZ_PROC --> TAL_SIL[talend_process_to_silver]
            TAL_SIL --> SIL_TABLES[silver_layer_tables]
            SIL_PROC[silver_procedure] --> SIL_TABLES
        end

        %% Gold layer
        subgraph Gold
            BRZ_PROC --> TAL_GOLD[talend_process_to_gold]
            TAL_GOLD --> GOLD_TABLES[gold_layer_tables]
            GOLD_PROC[gold_procedure] --> GOLD_TABLES
        end

        %% Spark + Hadoop
        subgraph Batch
            GOLD_TABLES --> SPARK[spark_batch_to_hadoop]
            SPARK --> HADOOP[hadoop_batch_processing]
        end

    end

    %% Styles
    style AIRFLOW fill:#ECEFF1,stroke:#37474F,stroke-width:3px,color:#000
    style KAG fill:#FFF9C4,stroke:#F57F17
    style PY  fill:#BBDEFB,stroke:#1976D2
    style BRZ_PROC fill:#B3E5FC,stroke:#0288D1

    style TAL_SIL fill:#FFE0B2,stroke:#F57C00
    style SIL_TABLES fill:#FFF3E0,stroke:#FF8F00
    style SIL_PROC fill:#FFE0B2,stroke:#F57C00

    style TAL_GOLD fill:#E1BEE7,stroke:#8E24AA
    style GOLD_TABLES fill:#FFFDE7,stroke:#FBC02D
    style GOLD_PROC fill:#E1BEE7,stroke:#7B1FA2

    style SPARK fill:#C8E6C9,stroke:#388E3C
    style HADOOP fill:#CFD8DC,stroke:#455A64
```

**Pipeline Duration**: ~4-6 minutes  
**Critical Path**: Spark ETL job takes ~4 minutes

---

## 🛠️ Technology Stack

### Data Technologies

| Technology       | Version | Role                | Why Chosen                                                              |
| ---------------- | ------- | ------------------- | ----------------------------------------------------------------------- |
| **Apache Spark** | 3.3.0   | ETL Engine          | Industry standard for big data processing, supports batch and streaming |
| **Apache Hive**  | 2.3.2   | Data Warehouse      | SQL interface, Parquet storage, integrates with Hadoop ecosystem        |
| **HDFS**         | 3.3.5   | Distributed Storage | Fault-tolerant, scalable storage for big data                           |
| **SQL Server**   | 2022    | Source Database     | Enterprise-grade RDBMS, supports advanced features                      |

### Orchestration & Infrastructure

| Technology         | Version | Role                   | Why Chosen                                        |
| ------------------ | ------- | ---------------------- | ------------------------------------------------- |
| **Apache Airflow** | 2.7.3   | Workflow Orchestration | Programmable workflows, rich UI, strong community |
| **Docker**         | 20.10+  | Containerization       | Consistent environments, easy deployment          |
| **PostgreSQL**     | 13      | Airflow Metadata       | Reliable metadata store for Airflow               |
| **Redis**          | Latest  | Message Broker         | Fast Celery executor backend for Airflow          |

## ✨ Features ✈️

### Core Capabilities

**📊 Complete ETL Pipeline: Automated data movement from local files/APIs through Bronze, Silver, and Gold layers in PostgreSQL**

**🔄 Workflow Orchestration: Apache Airflow manages complex, multi-stage DAGs with dependencies between Python ingestion and Talend transformation jobs.**

**⚡ High-Volume Ingestion: Python scripts use multi-threading and yield buffers for memory-efficient and fast loading of large datasets (e.g., 6M+ flight records).**

**🧩 Complex Transformation: Talend 8.0.1 handles data cleansing, standardization, and dimensional modeling, including pagination for large data flows.**

**🎯 Star Schema: Optimized dimensional model with 7 Dimension Tables and 1 Fact Table for high-performance flight analytics.**

**☁️ Data Enrichment: Integrated Weather API to enrich the DIM_WEATHER table, providing context for flight delays and operational analysis.**

**🐳 Containerized Database: The entire data warehouse is hosted on a Dockerized PostgreSQL instance for a stable and easily reproducible environment.**

**🔌 Direct Connectivity: JDBC/ODBC connectivity between Talend and the PostgreSQL data warehouse for efficient data manipulation.**

### Quick Start

### Batch Proccessing

```bash
docker compose up --build -d

# Wait for services to be healthy (2-3 minutes)
docker compose ps
```

### Stream Proccessing

```bash
#for spark consumer
docker cp scripts/pyspark/receiver.py spark-master:/opt/kafka_consumer.py

docker exec -it spark-master /spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 \
    --jars /opt/spark/jars/postgresql-42.7.7.jar \
   /opt/kafka_consumer.py


#for kafka producer
docker cp notebooks/producer.py spark-notebook:/tmp/kafka_streaming_job.py

docker exec -it spark-notebook pip install kafka-python


docker exec -it --user root spark-notebook /usr/local/spark/bin/spark-submit \
      --master spark://spark-master:7077 \
      --name NotebookStreamingConsumer \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 \
   /tmp/kafka_streaming_job.py

```

---

## 🔧 Troubleshooting

### Common Issues & Solutions

<details>
<summary><strong>🔴 Airflow tasks stuck in "queued"</strong></summary>

**Symptoms**: Tasks remain in "queued" state indefinitely

**Root Cause**: Redis not on same Docker network as Airflow

**Solution**:

```yaml
# In docker-compose.yml, ensure Redis has:
redis:
  networks:
    - kafka-net # Add this network
```

**Verify**:

```bash
docker exec etl-flight-airflow-worker-1 redis-cli -h redis ping
# Should return: PONG
```

</details>
