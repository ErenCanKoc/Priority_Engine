import pandas as pd
from config import OUTPUT_DIR

def run_llm_stage_1():
    df = pd.read_csv(f"{OUTPUT_DIR}/serp_output.csv")

    # TODO: LLM triage logic
    df["llm_stage_1_verdict"] = "monitor"
    df["llm_stage_1_confidence"] = "low"

    output_path = f"{OUTPUT_DIR}/llm_stage_1_output.csv"
    df.to_csv(output_path, index=False)
    print(f"✔ LLM Stage 1 output saved → {output_path}")