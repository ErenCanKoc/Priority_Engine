# src/llm_stage_1_triage.py
import os
import json
import time
import pandas as pd
from openai import OpenAI

from config import OUTPUT_DIR, OPENAI_API_KEY, LLM_MAX_ITEMS
from llm_prompts import TRIAGE_SYSTEM_PROMPT

MODEL_STAGE_1 = "gpt-4.1-mini"
TEMPERATURE = 0.2
SLEEP = 0.35  # 11k için rate-limit durumuna göre artır/azalt

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

def run_llm_stage_1():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    in_path = os.path.join(OUTPUT_DIR, "serp_output.csv")
    df = pd.read_csv(in_path)

    # placeholders
    df["llm_stage_1_verdict"] = ""
    df["llm_stage_1_confidence"] = ""
    df["llm_stage_1_reasoning"] = ""
    df["llm_stage_1_risk_flags"] = ""

    # run on all rows (optionally cap with LLM_MAX_ITEMS for testing)
    target = df.head(LLM_MAX_ITEMS)

    for idx, r in target.iterrows():
        try:
            user_input = _build_user_input(r)

            resp = client.chat.completions.create(
                model=MODEL_STAGE_1,
                messages=[
                    {"role": "system", "content": TRIAGE_SYSTEM_PROMPT},
                    {"role": "user", "content": user_input},
                ],
                temperature=TEMPERATURE,
            )

            raw = resp.choices[0].message.content.strip()

            try:
                parsed = json.loads(raw)
            except json.JSONDecodeError:
                # fallback
                df.loc[idx, "llm_stage_1_verdict"] = "monitor"
                df.loc[idx, "llm_stage_1_confidence"] = "low"
                df.loc[idx, "llm_stage_1_reasoning"] = raw[:500]
                df.loc[idx, "llm_stage_1_risk_flags"] = "json_parse_failed"
                time.sleep(SLEEP)
                continue

            df.loc[idx, "llm_stage_1_verdict"] = parsed.get("verdict", "")
            df.loc[idx, "llm_stage_1_confidence"] = parsed.get("confidence", "")
            df.loc[idx, "llm_stage_1_reasoning"] = parsed.get("reasoning", "")
            df.loc[idx, "llm_stage_1_risk_flags"] = "; ".join(parsed.get("risk_flags", []))

            time.sleep(SLEEP)

        except Exception as e:
            df.loc[idx, "llm_stage_1_verdict"] = "monitor"
            df.loc[idx, "llm_stage_1_confidence"] = "low"
            df.loc[idx, "llm_stage_1_reasoning"] = f"llm_error:{str(e)}"
            df.loc[idx, "llm_stage_1_risk_flags"] = "llm_error"

    out_path = os.path.join(OUTPUT_DIR, "llm_stage_1_output.csv")
    df.to_csv(out_path, index=False)
    print(f"✔ LLM Stage 1 output saved → {out_path}")
