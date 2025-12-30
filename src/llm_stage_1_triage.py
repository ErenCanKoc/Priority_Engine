# src/llm_stage_1_triage.py
import os
import json
import time
import pandas as pd
from openai import OpenAI

from config import OUTPUT_DIR, OPENAI_API_KEY, LLM_MAX_ITEMS
from llm_prompts import TRIAGE_SYSTEM_PROMPT

MODEL_STAGE_1 = "gpt-4o-mini"
TEMPERATURE = 0.2
SLEEP = 0.4

client = OpenAI(api_key=OPENAI_API_KEY)


# -----------------------
# Build LLM input (explicit)
# -----------------------
def _build_user_input(r):
    return f"""
URL: {r.get('url','')}
Query: {r.get('keyword','')}

Analyze candidate: {r.get('analyze_candidate','')}
Candidate type: {r.get('candidate_type','')}
Candidate reason: {r.get('candidate_reason','')}
Engine status: {r.get('engine_status','')}

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
Traffic gap (clicks): {r.get('traffic_gap','')}

SERP status: {r.get('serp_status','')}
SERP features: {r.get('serp_features','')}
SERP rank: {r.get('serp_rank','')}
SERP competition: {r.get('serp_competition','')}
SERP error: {r.get('serp_error','')}
""".strip()


# -----------------------
# Main runner (V3)
# -----------------------
def run_llm_stage_1():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    in_path = os.path.join(OUTPUT_DIR, "serp_output.csv")
    df = pd.read_csv(in_path)

    # Guarantee output columns (structured reasoning fields)
    for c in [
        "llm_stage_1_status",
        "llm_stage_1_verdict",
        "llm_stage_1_confidence",
        "llm_stage_1_problem",
        "llm_stage_1_cause",
        "llm_stage_1_opportunity",
        "llm_stage_1_evidence",
        "llm_stage_1_reasoning_raw",  # backup: full reasoning as JSON string
        "llm_stage_1_risk_flags",
    ]:
        if c not in df.columns:
            df[c] = ""

    # Defaults
    df["llm_stage_1_status"] = "skipped"
    df["llm_stage_1_verdict"] = "ignore"
    df["llm_stage_1_confidence"] = "low"
    df["llm_stage_1_problem"] = ""
    df["llm_stage_1_cause"] = ""
    df["llm_stage_1_opportunity"] = ""
    df["llm_stage_1_evidence"] = ""
    df["llm_stage_1_reasoning_raw"] = ""
    df["llm_stage_1_risk_flags"] = ""

    # Gate: only analyze_candidate=True goes to LLM
    target = df[df.get("analyze_candidate", False) == True].copy()

    # Optional cap for safety
    if LLM_MAX_ITEMS and LLM_MAX_ITEMS > 0:
        target = target.head(LLM_MAX_ITEMS)

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
                # Hard fallback: never block downstream
                df.loc[idx, "llm_stage_1_status"] = "error"
                df.loc[idx, "llm_stage_1_verdict"] = "monitor"
                df.loc[idx, "llm_stage_1_confidence"] = "low"
                df.loc[idx, "llm_stage_1_reasoning_raw"] = raw[:500]
                df.loc[idx, "llm_stage_1_risk_flags"] = "json_parse_failed"
                time.sleep(SLEEP)
                continue

            verdict = parsed.get("verdict", "monitor")
            confidence = parsed.get("confidence", "low")
            reasoning = parsed.get("reasoning", {})
            risk_flags = parsed.get("risk_flags", [])

            # Parse structured reasoning (with fallbacks)
            if isinstance(reasoning, dict):
                problem = reasoning.get("problem", "")
                cause = reasoning.get("cause", "")
                opportunity = reasoning.get("opportunity", "")
                evidence = reasoning.get("evidence", "")
            elif isinstance(reasoning, str):
                # Fallback: LLM returned string instead of dict
                problem = reasoning[:200]
                cause = ""
                opportunity = ""
                evidence = ""
                risk_flags = list(set(risk_flags + ["reasoning_format_error"]))
            else:
                problem = ""
                cause = ""
                opportunity = ""
                evidence = ""
                risk_flags = list(set(risk_flags + ["reasoning_missing"]))

            # Enforce SERP-aware confidence (contract)
            if r.get("serp_status") != "ok":
                confidence = "low"
                risk_flags = list(set(risk_flags + ["serp_unavailable"]))

            df.loc[idx, "llm_stage_1_status"] = "ok"
            df.loc[idx, "llm_stage_1_verdict"] = verdict
            df.loc[idx, "llm_stage_1_confidence"] = confidence
            df.loc[idx, "llm_stage_1_problem"] = problem
            df.loc[idx, "llm_stage_1_cause"] = cause
            df.loc[idx, "llm_stage_1_opportunity"] = opportunity
            df.loc[idx, "llm_stage_1_evidence"] = evidence
            df.loc[idx, "llm_stage_1_reasoning_raw"] = json.dumps(reasoning) if reasoning else ""
            df.loc[idx, "llm_stage_1_risk_flags"] = "; ".join(risk_flags)

            time.sleep(SLEEP)

        except Exception as e:
            df.loc[idx, "llm_stage_1_status"] = "error"
            df.loc[idx, "llm_stage_1_verdict"] = "monitor"
            df.loc[idx, "llm_stage_1_confidence"] = "low"
            df.loc[idx, "llm_stage_1_reasoning_raw"] = f"llm_error:{str(e)}"
            df.loc[idx, "llm_stage_1_risk_flags"] = "llm_error"

    out_path = os.path.join(OUTPUT_DIR, "llm_stage_1_output.csv")
    df.to_csv(out_path, index=False)
    print(f"✓ LLM Stage 1 V3 output saved → {out_path}")
