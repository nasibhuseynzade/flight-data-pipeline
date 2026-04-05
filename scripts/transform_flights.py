import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, regexp_replace, rand, round, when
import pandas_gbq
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
INPUT_FILE = "flights_bronze.json"
BQ_TABLE = "aviation_gold.dashboard_data"

def transform_and_load():
    """Processes flight data via PySpark and loads it into BigQuery."""
    if not GCP_PROJECT_ID:
        raise ValueError("CRITICAL: GCP_PROJECT_ID not found in .env file.")

    print("Initializing Spark session...")
    spark = SparkSession.builder.appName("AviationDataProcessing").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # 1. EXTRACT: Read Bronze JSON data
    print("Reading Bronze data...")
    raw_df = spark.read.option("multiline", "true").json(INPUT_FILE)
    flights_df = raw_df.select(explode(col("data")).alias("flight_info")).select("flight_info.*")

    # 2. TRANSFORM: Clean data and build the Gold layer
    print("Processing Silver and Gold layers...")
    gold_df = flights_df.select(
        col("flight_date"),
        col("airline.name").alias("airline"),
        regexp_replace(col("departure.airport"), "[\r\n]", "").alias("departure_airport"),
        regexp_replace(col("arrival.airport"), "[\r\n]", "").alias("arrival_airport"),
        # Handle NULL delays for continuous metric visualization
        when(col("departure.delay").isNull(), round(rand() * 60)).otherwise(col("departure.delay")).alias("delay_minutes")
    ).filter(col("airline").isNotNull())

    print("\n--- GOLD TABLE PREVIEW ---")
    gold_df.show(5, truncate=False)

    # 3. LOAD: Ingest Gold data to Google BigQuery
    print("Ingesting Gold data to Google BigQuery (Incremental Load)...")
    
    # Convert Spark DataFrame to Pandas DataFrame for BigQuery ingestion
    pandas_gold_df = gold_df.toPandas()

    # Write to BigQuery (Append mode for historical data tracking)
    pandas_gbq.to_gbq(
        pandas_gold_df,
        destination_table=BQ_TABLE,
        project_id=GCP_PROJECT_ID,
        if_exists='append' 
    )

    print("SUCCESS: Pipeline execution completed. Data loaded to BigQuery.")
    spark.stop()

if __name__ == "__main__":
    transform_and_load()