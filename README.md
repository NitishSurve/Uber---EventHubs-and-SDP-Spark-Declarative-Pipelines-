🚖 End-to-End Data Engineering Project – Uber Ride Analytics

This project demonstrates a complete end-to-end Data Engineering pipeline built using Databricks, Azure Data Factory, Azure Event Hubs, PySpark, Spark Structured Streaming, and Spark Declarative Pipelines.

The pipeline processes both batch and real-time ride data, implementing a modern Lakehouse architecture with the Medallion pattern (Bronze → Silver → Gold).

🏗️ Architecture
Data Sources (JSON + Event Hubs)
        │
        ▼
Bronze Layer (Raw Ingestion)
        │
        ▼
Silver Layer (Transformations & OBT)
        │
        ▼
Gold Layer (Fact & Dimension Tables)
⚙️ Tech Stack

🧠 Databricks

⚡ PySpark

🔄 Spark Structured Streaming

🏗️ Spark Declarative Pipelines

📡 Azure Event Hubs

🗄️ Delta Lake

🔧 Jinja SQL Templates

📥 Data Ingestion

Batch ingestion from JSON datasets into the Bronze layer. 

bronze_adls

Real-time ride events streamed from Azure Event Hubs using Spark Structured Streaming. 

ingest

🥈 Silver Layer

Raw ride events are parsed, cleaned, and enriched using PySpark streaming transformations. 

silver

The project also generates a Silver Operational Business Table (OBT) by joining ride data with vehicle metadata. 

silver_obt

🥇 Gold Layer

A dimensional data model is created using CDC pipelines to build:

Passenger dimension

Driver dimension

Vehicle dimension

Payment dimension

Location dimension

Fact table for ride metrics 

model

🚀 Key Concepts Demonstrated

✔ Medallion Architecture
✔ Real-time data streaming
✔ CDC-based dimensional modeling
✔ Lakehouse architecture
✔ Batch + streaming pipelines
