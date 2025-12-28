import pandas as pd
from config import INPUT_FILE, OUTPUT_DIR

def run_engine():
    df = pd.read_csv(INPUT_FILE)

    # TODO: priority engine logic buraya gelecek
    df["engine_suggestion"] = "monitor"

    output_path = f"{OUTPUT_DIR}/engine_output.csv"
    df.to_csv(output_path, index=False)
    print(f"✔ Engine output saved → {output_path}")