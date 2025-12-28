import pandas as pd
from serpapi import GoogleSearch
from config import OUTPUT_DIR, SERPAPI_KEY, SERP_LIMIT

def run_serp():
    df = pd.read_csv(f"{OUTPUT_DIR}/engine_output.csv")

    # TODO: SERP logic buraya gelecek
    df["serp_checked"] = False
    df["serp_summary"] = ""

    output_path = f"{OUTPUT_DIR}/serp_output.csv"
    df.to_csv(output_path, index=False)
    print(f"✔ SERP output saved → {output_path}")