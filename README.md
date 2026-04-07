# ✈️ Serverless Aviation ETL Data Pipeline with PySpark & GCP 

This repository contains an end-to-end, production-ready data engineering pipeline that ingests live flight data, processes it using distributed computing patterns, and exposes it for real-time analytics.

The project demonstrates a pragmatic integration of **distributed processing (PySpark)** with **serverless infrastructure (Google Cloud Run Jobs)** and modern **DevOps (GitHub Actions CI/CD)**.

## 📌 Project Architecture Overview

This pipeline follows the modern **Medallion Architecture** (Lakehouse pattern), automating the flow of data from a REST API to a cloud data warehouse on an hourly schedule.

![Data Pipeline Architecture Diagram](aviation_pipeline.png)
*(Note: Replace `path/to/your/architecture_image.png` with the actual path or URL to your generated architecture diagram)*

### Key Engineering Highlights:
* **Infrastructure:** Fully serverless (GCP Cloud Run Jobs), zero idle costs.
* **Processing:** PySpark for scalable transformations. 
* **Data Modeling:** Medallion Architecture (Bronze, Silver, Gold layers).
* **Domain Logic:** Probability-based synthetic data generation for missing values.
* **DevOps:** Full CI/CD pipeline using GitHub Actions with secure IAM integration.

---

## ✨ Technology Stack

| Component | Technology | Description |
| :--- | :--- | :--- |
| **Language** | Python 3.11 | Core scripting and logic. |
| **Distributed Compute** | **Apache Spark (PySpark)** | Data transformation engine (Medallion layers). |
| **Containerization** | **Docker** | Encapsulating Python, JRE, and dependencies. |
| **Cloud Provider** | **Google Cloud Platform (GCP)** | Hosting infrastructure. |
| **Serverless Compute** | **Cloud Run Jobs** | Executing the ETL container on demand. |
| **Data Warehouse** | **BigQuery** | Scalable storage for processed Gold data. |
| **Scheduler** | **Cloud Scheduler** | Triggering the hourly ETL job. |
| **CI/CD** | **GitHub Actions** | Automated build, test, and deploy workflow. |
| **BI / Visualization** | **Looker Studio** | Real-time interactive dashboard. |
| **Source API** | Aviationstack API | Live flight data source. |

---

## 📉 Data Pipeline Flow (Medallion Architecture)

### 1. Ingestion (Bronze Layer)
The Python script makes an authenticated request to the `Aviationstack API`. The raw JSON response is read directly into a **PySpark DataFrame**.
* **Data Status:** Raw, unvalidated, historical landing zone.

### 2. Transformation & Cleaning (Silver Layer)
PySpark is used to enforce schema and clean the data.
* **Operations:** Filtering (specifically locked to Frankfurt Airport - **FRA**), cleaning whitespace and newline characters from airport names using `regexp_replace`.
* **Data Status:** Cleaned, filtered, structure enforced.

### 3. Loading (Data Warehouse)
The Pandas Gold DataFrame is appended to a **Google BigQuery** table (`aviation_gold.dashboard_data`). A `processed_at` timestamp is added to track data freshness.

---

## 🚀 DevOps & CI/CD Pipeline

The project is fully automated using **GitHub Actions**. Every `git push` to the `main` branch triggers the following workflow:

1.  **Authentication:** Securely authenticates with GCP using a dedicated Service Account Key (`GCP_SA_KEY` stored in GitHub Secrets).
2.  **Container Build:** GitHub instructs GCP **Cloud Build** to build a new Docker image based on the `Dockerfile`.
    * *Dockerfile Note:* The custom `Dockerfile` explicitly installs the **Java Runtime Environment (default-jre)** needed for PySpark, using a stable `python:3.11-slim` base image to prevent build failures associated with cutting-edge Python versions.
3.  **Deploy:** The updated container image is deployed to **GCP Cloud Run Jobs** (`aviation-etl-job`).
4.  **Security:** All API keys and project IDs are injected into the container at runtime via environmental variables, ensuring no secrets are exposed in the source code.

---

## 🛠️ Installation & Local Setup

### Prerequisites
* Python 3.11
* Java (JRE) installed (required for PySpark locally)
* GCP Account and Project
* Aviationstack API Key

### Steps
1.  **Clone the Repository:**
    ```bash
    git clone [https://github.com/yourusername/flight-data-pipeline.git](https://github.com/yourusername/flight-data-pipeline.git)
    cd flight-data-pipeline
    ```
2.  **Set up Virtual Environment:**
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows: venv\Scripts\activate
    pip install -r requirements.txt
    ```
3.  **Configure Environment Variables:** Create a `.env` file in the root directory:
    ```env
    GCP_PROJECT_ID=your-gcp-project-id
    AVIATION_API_KEY=your-aviationstack-api-key
    INPUT_FILE=flights_bronze.json 
    ```
4.  **Run the Pipeline Locally:**
    ```bash
    python main.py
    ```

---

## 📊 Business Intelligence & Visualization

The final Gold data in BigQuery is visualized using a real-time **Looker Studio Dashboard**.

### Dashboard Highlights:
* **Frankfurt Airport (FRA) Outbound Analytics:** Focused operational view.
* **Live Data Refreshment:** Visualizes data on an hourly cache, matching the scheduler.
* **Key Performance Indicators (KPIs):** Total Flights, Average Delay, Maximum Delay.
* **Operational Distributions:** A donut chart visualizes the On-Time vs. Delayed flight segments, accurately reflecting the synthetic data logic applied in the Gold layer.
* **Airline Performance:** Bar charts showing average delays per airline, answering "Which carrier is least reliable at FRA?".

---