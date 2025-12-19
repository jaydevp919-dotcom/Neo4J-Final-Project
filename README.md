# ✈️ Neo4j Airline Big Data Analytics Platform

## 📌 Project Overview
This project implements an **end-to-end Big Data analytics pipeline** using **Neo4j** as a distributed graph database.  
It ingests, cleans, validates, aggregates, and visualizes **large-scale airline flight data** following a **Medallion Architecture**.

The system demonstrates real-world Big Data concepts such as scalable ingestion, schema validation, graph modeling, indexing, and analytical dashboards.

---

## 🧠 Technologies Used
- **Neo4j** (Graph Database)
- **Python 3.11**
- **Pandas**
- **Pydantic** (Schema validation)
- **PyTest** (Unit testing)
- **MyPy** (Static type checking)
- **Streamlit** (Dashboard)
- **Neo4j Desktop / Docker**
- **Mermaid** (Architecture diagram)

---

## 📊 Dataset
- Airline on-time performance dataset
- **1M+ rows**
- **20+ meaningful columns**
- Format: CSV  
- Fields include flight date, carrier, origin/destination, delays, cancellations, distance
Dataset Link : https://www.kaggle.com/datasets/sherrytp/airline-delay-analysis #20.csv
---


## 🏗️ Architecture (Medallion Pattern)

```mermaid
flowchart LR
  A["Airline CSV Dataset (20.csv)"] --> B["Python Ingestion (Pandas + UV)"]
  B --> C[("Neo4j Raw Layer: RawFlight")]
  C --> D[("Neo4j Clean Layer: Flight")]
  D --> E[("Neo4j Gold Layer: Aggregated Summaries")]
  E --> F["Streamlit Dashboard"]
  B -->|"Pydantic Validation"| D
  D -->|"Cypher Aggregations"| E
---

# ▶️ How to Run the Project (Step-by-Step)
## Step 1: Clone the Repository
```bash
git clone https://github.com/<your-username>/neo4j-airline-bigdata.git
cd neo4j-airline-bigdata

Step 2: Start Neo4j
Option A: Neo4j Desktop

Open Neo4j Desktop

Create a new project

Start a Neo4j database

Set credentials:

Username: neo4j
Password: neo4jpassword

Step 3: Install Project Dependencies
From the project root directory:
uv sync

Step 4: Place the Dataset
Create a folder named data
Place the airline CSV file inside it:
data/20.csv

Step 5: Ingest Raw Data into Neo4j
python -m src.raw.ingest_raw data/20.csv

Step 7: Build the Gold Layer (Aggregations)
python -m src.gold.build_gold

Step 8: Run the Streamlit Dashboard
streamlit run src/viz/app.py

Step 9: (Optional) Run Tests
uv run pytest
uv run mypy src

