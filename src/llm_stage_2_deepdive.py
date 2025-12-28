import pandas as pd
from config import OUTPUT_DIR

def run_llm_stage_2():
    df = pd.read_csv(f"{OUTPUT_DIR}/llm_stage_1_output.csv")

    # TODO: Only rows with action + high/medium confidence
    df["llm_final_verdict"] = ""
    df["llm_final_actions"] = ""

    output_path = f"{OUTPUT_DIR}/final_output.csv"
    df.to_csv(output_path, index=False)
    print(f"✔ Final output saved → {output_path}")
