import os
import json
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
API_KEY = os.getenv("AVIATION_API_KEY")
BASE_URL = "http://api.aviationstack.com/v1/flights"
OUTPUT_FILE = "flights_bronze.json"

def extract_data():
    """Fetches real-time flight data from AviationStack API."""
    if not API_KEY:
        raise ValueError("CRITICAL: API Key not found in .env file.")

    params = {
        'access_key': API_KEY,
        'limit': 100  # Fetching 100 records for pipeline processing
    }

    print("Fetching raw data from AviationStack API...")
    response = requests.get(BASE_URL, params=params)

    if response.status_code == 200:
        raw_data = response.json()
        
        # Save raw data as Bronze layer
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            json.dump(raw_data, f, ensure_ascii=False, indent=4)
            
        print(f"SUCCESS: Raw data extracted and saved to '{OUTPUT_FILE}'.")
    else:
        raise Exception(f"FAILED: API returned status code {response.status_code}. Details: {response.text}")

if __name__ == "__main__":
    extract_data()