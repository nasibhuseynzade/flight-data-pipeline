import subprocess

def run_pipeline():
    print("Data Extraction started...")
    subprocess.run(["python", "scripts/extract_flights.py"], check=True)
    
    print("Data Transform & Load started...")

    subprocess.run(["python", "scripts/transform_flights.py"], check=True)
    
    print("Done")

if __name__ == "__main__":
    run_pipeline()