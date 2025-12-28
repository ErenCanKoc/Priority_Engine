# src/llm_stage_2_deepdive.py
import os
import json
import time
import pandas as pd
from openai import OpenAI

from config import OUTPUT_DIR, OPENAI_API_KEY
from llm_prompts import DEEPDIVE_SYSTEM_PROMPT

MODEL_STAGE_2 = "gpt-4.1-mini"
TEMPERATURE = 0.2
SLEEP = 0.9  # daha az satır, daha derin

client = OpenAI(api_key=OPENAI_API_KEY)

def _build_user_input(r):
    return f"""
URL: {r.get('url','')}
Query: {r.get('keyword','')}

Page type: {r.get('page_type','')}
Query type: {r.get('query_type','')}
Problem type: {r.get('problem_type','')}

Clicks (current): {r.get('clicks_last','')}
Clicks (previous): {r.get('clicks_prev','')}
Impressions (current): {r.get('impr_last','')}
Impressions (previous): {r.get('impr_prev','')}
Avg position: {r.get('pos','')}

Estimated MSV (monthly): {r.get('msv_est','')}
Utilization ratio: {r.get('utilization','')}
Traffic gap: {r.get('traffic_gap','')}

Engine suggestion: {r.get('engine_suggestion','')}
Engine reason: {r.get('suggestion_reason','')}

SERP available: {r.get('serp_data_available','')}
SERP summary: {r.get('serp_summary','')}
""".strip()

def run_llm_stage_2():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    in_path = os.path.join(OUTPUT_DIR, "llm_stage_1_output.csv")
    df = pd.read_csv(in_path)

    # create final columns
    df["llm_final_verdict"] = ""
    df["llm_final_confidence"] = ""
    df["llm_final_reasoning"] = ""
    df["llm_final_actions"] = ""
    df["llm_final_risk_flags"] = ""

    # stage2 needed: verdict=action AND confidence != low
    stage2 = df[
        (df["llm_stage_1_verdict"] == "action") &
        (df["llm_stage_1_confidence"].isin(["high", "medium"]))
    ].copy()

    for idx, r in stage2.iterrows():
        try:
            user_input = _build_user_input(r)

            resp = client.chat.completions.create(
                model=MODEL_STAGE_2,
                messages=[
                    {"role": "system", "content": DEEPDIVE_SYSTEM_PROMPT},
                    {"role": "user", "content": user_input},
                ],
                temperature=TEMPERATURE,
            )

            raw = resp.choices[0].message.content.strip()

            try:
                parsed = json.loads(raw)
            except json.JSONDecodeError:
                df.loc[idx, "llm_final_verdict"] = "monitor"
                df.loc[idx, "llm_final_confidence"] = "low"
                df.loc[idx, "llm_final_reasoning"] = raw[:1000]
                df.loc[idx, "llm_final_risk_flags"] = "json_parse_failed"
                time.sleep(SLEEP)
                continue

            df.loc[idx, "llm_final_verdict"] = parsed.get("verdict", "")
            df.loc[idx, "llm_final_confidence"] = parsed.get("confidence", "")
            df.loc[idx, "llm_final_reasoning"] = parsed.get("reasoning", "")
            df.loc[idx, "llm_final_actions"] = "; ".join(parsed.get("recommended_actions", []))
            df.loc[idx, "llm_final_risk_flags"] = "; ".join(parsed.get("risk_flags", []))

            time.sleep(SLEEP)

        except Exception as e:
            df.loc[idx, "llm_final_verdict"] = "monitor"
            df.loc[idx, "llm_final_confidence"] = "low"
            df.loc[idx, "llm_final_reasoning"] = f"llm_error:{str(e)}"
            df.loc[idx, "llm_final_risk_flags"] = "llm_error"

    out_path = os.path.join(OUTPUT_DIR, "final_output.csv")
    df.to_csv(out_path, index=False)
    print(f"✔ Final output saved → {out_path}")
