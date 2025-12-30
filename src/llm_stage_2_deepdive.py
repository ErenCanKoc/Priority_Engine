# src/llm_stage_2_deepdive.py
import os
import json
import time
import pandas as pd
from openai import OpenAI

from config import OUTPUT_DIR, OPENAI_API_KEY
from llm_prompts import DEEPDIVE_SYSTEM_PROMPT

MODEL_STAGE_2 = "gpt-4o-mini"
TEMPERATURE = 0.2
SLEEP = 0.9

client = OpenAI(api_key=OPENAI_API_KEY)


# -----------------------
# Build LLM input (deep)
# -----------------------
def _build_user_input(r):
    return f"""
URL: {r.get('url','')}
Query: {r.get('keyword','')}

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

LLM Stage 1 verdict: {r.get('llm_stage_1_verdict','')}
LLM Stage 1 confidence: {r.get('llm_stage_1_confidence','')}
LLM Stage 1 problem: {r.get('llm_stage_1_problem','')}
LLM Stage 1 cause: {r.get('llm_stage_1_cause','')}
LLM Stage 1 opportunity: {r.get('llm_stage_1_opportunity','')}

SERP status: {r.get('serp_status','')}
SERP features: {r.get('serp_features','')}
SERP rank: {r.get('serp_rank','')}
SERP competition: {r.get('serp_competition','')}
SERP error: {r.get('serp_error','')}
""".strip()


# -----------------------
# Main runner (V3)
# -----------------------
def run_llm_stage_2():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    in_path = os.path.join(OUTPUT_DIR, "llm_stage_1_output.csv")
    df = pd.read_csv(in_path)

    # Guarantee output columns (structured reasoning + actions)
    for c in [
        "llm_stage_2_status",
        "llm_final_verdict",
        "llm_final_confidence",
        "llm_final_problem",
        "llm_final_cause",
        "llm_final_opportunity",
        "llm_final_evidence",
        "llm_final_reasoning_raw",  # backup: full reasoning as JSON string
        "llm_final_actions",
        "llm_final_risk_flags",
    ]:
        if c not in df.columns:
            df[c] = ""

    # Defaults
    df["llm_stage_2_status"] = "skipped"
    df["llm_final_verdict"] = "monitor"
    df["llm_final_confidence"] = "low"
    df["llm_final_problem"] = ""
    df["llm_final_cause"] = ""
    df["llm_final_opportunity"] = ""
    df["llm_final_evidence"] = ""
    df["llm_final_reasoning_raw"] = ""
    df["llm_final_actions"] = ""
    df["llm_final_risk_flags"] = ""

    # Gate: only rows explicitly marked as action
    stage2 = df[df["llm_stage_1_verdict"] == "action"].copy()

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
                # JSON fail should NEVER kill pipeline
                df.loc[idx, "llm_stage_2_status"] = "error"
                df.loc[idx, "llm_final_verdict"] = "monitor"
                df.loc[idx, "llm_final_confidence"] = "low"
                df.loc[idx, "llm_final_reasoning_raw"] = raw[:1000]
                df.loc[idx, "llm_final_risk_flags"] = "json_parse_failed"
                time.sleep(SLEEP)
                continue

            verdict = parsed.get("verdict", "monitor")
            confidence = parsed.get("confidence", "low")
            reasoning = parsed.get("reasoning", {})
            actions = parsed.get("recommended_actions", [])
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

            # Enforce risk flags based on upstream context
            if r.get("serp_status") != "ok":
                risk_flags = list(set(risk_flags + ["serp_unavailable"]))

            if r.get("llm_stage_1_confidence") == "low":
                risk_flags = list(set(risk_flags + ["low_upstream_confidence"]))

            # Validate actions specificity (basic check)
            generic_phrases = ["improve content", "optimize title", "build links", "add keywords"]
            if actions:
                for action in actions:
                    if any(phrase in action.lower() for phrase in generic_phrases):
                        if len(action.split()) < 8:  # if action is too short, likely generic
                            risk_flags = list(set(risk_flags + ["generic_actions"]))
                            break

            df.loc[idx, "llm_stage_2_status"] = "ok"
            df.loc[idx, "llm_final_verdict"] = verdict
            df.loc[idx, "llm_final_confidence"] = confidence
            df.loc[idx, "llm_final_problem"] = problem
            df.loc[idx, "llm_final_cause"] = cause
            df.loc[idx, "llm_final_opportunity"] = opportunity
            df.loc[idx, "llm_final_evidence"] = evidence
            df.loc[idx, "llm_final_reasoning_raw"] = json.dumps(reasoning) if reasoning else ""
            df.loc[idx, "llm_final_actions"] = " | ".join(actions) if actions else ""
            df.loc[idx, "llm_final_risk_flags"] = "; ".join(risk_flags)

            time.sleep(SLEEP)

        except Exception as e:
            df.loc[idx, "llm_stage_2_status"] = "error"
            df.loc[idx, "llm_final_verdict"] = "monitor"
            df.loc[idx, "llm_final_confidence"] = "low"
            df.loc[idx, "llm_final_reasoning_raw"] = f"llm_error:{str(e)}"
            df.loc[idx, "llm_final_risk_flags"] = "llm_error"

    out_path = os.path.join(OUTPUT_DIR, "final_output.csv")
    df.to_csv(out_path, index=False)
    print(f"✓ LLM Stage 2 V3 final output saved → {out_path}")
