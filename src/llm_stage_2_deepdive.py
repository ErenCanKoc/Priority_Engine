# src/llm_stage_2_deepdive.py
import os
import json
import time
import logging
import pandas as pd
from datetime import datetime
from openai import OpenAI
from tqdm import tqdm

from config import OUTPUT_DIR, OPENAI_API_KEY
from llm_prompts import DEEPDIVE_SYSTEM_PROMPT

MODEL_STAGE_2 = "gpt-4o-mini"
TEMPERATURE = 0.2
SLEEP = 0.9

client = OpenAI(api_key=OPENAI_API_KEY)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(OUTPUT_DIR, 'llm_stage_2.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# -----------------------
# Metrics tracking
# -----------------------
class Stage2Metrics:
    def __init__(self):
        self.start_time = datetime.now()
        self.total_rows = 0
        self.processed_rows = 0
        self.success_count = 0
        self.error_count = 0
        self.json_parse_errors = 0
        self.api_errors = 0
        self.total_tokens_input = 0
        self.total_tokens_output = 0
        self.total_cost = 0.0
        
        # Verdict breakdown
        self.verdict_counts = {"action": 0, "monitor": 0, "ignore": 0}
        self.confidence_counts = {"high": 0, "medium": 0, "low": 0}
        self.generic_action_count = 0
        
        # Timing
        self.total_api_time = 0.0
        self.avg_api_time = 0.0
        
    def record_success(self, verdict, confidence, tokens_in, tokens_out, api_time, has_generic_actions):
        self.success_count += 1
        self.processed_rows += 1
        
        if verdict in self.verdict_counts:
            self.verdict_counts[verdict] += 1
        if confidence in self.confidence_counts:
            self.confidence_counts[confidence] += 1
        
        if has_generic_actions:
            self.generic_action_count += 1
            
        self.total_tokens_input += tokens_in
        self.total_tokens_output += tokens_out
        
        # GPT-4o-mini pricing: $0.15/1M input, $0.60/1M output
        self.total_cost += (tokens_in * 0.15 / 1_000_000) + (tokens_out * 0.60 / 1_000_000)
        
        self.total_api_time += api_time
        self.avg_api_time = self.total_api_time / self.success_count
        
    def record_error(self, error_type):
        self.error_count += 1
        self.processed_rows += 1
        
        if error_type == "json_parse":
            self.json_parse_errors += 1
        elif error_type == "api":
            self.api_errors += 1
    
    def get_summary(self):
        elapsed = (datetime.now() - self.start_time).total_seconds()
        
        return {
            "total_rows": self.total_rows,
            "processed_rows": self.processed_rows,
            "success_count": self.success_count,
            "error_count": self.error_count,
            "success_rate": f"{(self.success_count / max(self.processed_rows, 1)) * 100:.1f}%",
            
            "json_parse_errors": self.json_parse_errors,
            "api_errors": self.api_errors,
            "generic_action_warnings": self.generic_action_count,
            
            "verdict_action": self.verdict_counts["action"],
            "verdict_monitor": self.verdict_counts["monitor"],
            "verdict_ignore": self.verdict_counts["ignore"],
            
            "confidence_high": self.confidence_counts["high"],
            "confidence_medium": self.confidence_counts["medium"],
            "confidence_low": self.confidence_counts["low"],
            
            "total_tokens_input": self.total_tokens_input,
            "total_tokens_output": self.total_tokens_output,
            "total_cost_usd": f"${self.total_cost:.4f}",
            
            "total_time_seconds": f"{elapsed:.1f}",
            "avg_api_time_seconds": f"{self.avg_api_time:.2f}",
            "rows_per_minute": f"{(self.processed_rows / max(elapsed / 60, 0.01)):.1f}",
        }
    
    def log_summary(self):
        summary = self.get_summary()
        logger.info("=" * 80)
        logger.info("LLM STAGE 2 METRICS SUMMARY")
        logger.info("=" * 80)
        for key, value in summary.items():
            logger.info(f"{key:30s}: {value}")
        logger.info("=" * 80)
        
        return summary


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
# Column mapping & export helpers
# -----------------------
COLUMN_MAPPING = {
    "keyword": "Search_Term",
    "url": "Page_URL",
    "clicks_last": "Current_Clicks",
    "clicks_prev": "Previous_Clicks",
    "clicks_pct": "Clicks_Change_Percent",
    "impr_last": "Current_Impressions",
    "impr_prev": "Previous_Impressions",
    "impr_pct": "Impressions_Change_Percent",
    "ctr_last": "Current_Click_Rate",
    "ctr_pct": "Click_Rate_Change_Percent",
    "pos": "Average_Rank_Position",
    "pos_pct": "Rank_Change_Percent",
    "msv_est": "Monthly_Search_Volume",
    "utilization": "Potential_Captured_Percent",
    "traffic_gap": "Missing_Clicks_Per_Month",
    "expected_clicks": "Expected_Clicks_For_Rank",
    "page_type": "Page_Type",
    "query_type": "Search_Type",
    "problem_type": "Traffic_Pattern",
    "engine_status": "Data_Quality_Status",
    "analyze_candidate": "Selected_For_Analysis",
    "candidate_type": "Opportunity_Category",
    "candidate_reason": "Why_Selected",
    "data_ok": "Has_Complete_Data",
    "data_issues": "Data_Quality_Issues",
    "rescue_score": "Rescue_Priority_Score",
    "scale_score": "Growth_Priority_Score",
    "serp_status": "Google_Search_Check_Status",
    "serp_features": "Google_Features_Present",
    "serp_rank": "Actual_Rank_In_Google",
    "serp_competition": "Competition_Level",
    "serp_data_available": "Has_Google_Search_Data",
    "serp_error": "Google_Check_Error",
    "llm_stage_1_status": "Initial_Review_Status",
    "llm_stage_1_verdict": "Initial_Decision",
    "llm_stage_1_confidence": "Initial_Confidence_Level",
    "llm_stage_1_problem": "What_Is_Happening",
    "llm_stage_1_cause": "Why_Is_It_Happening",
    "llm_stage_1_opportunity": "What_We_Could_Gain",
    "llm_stage_1_evidence": "Supporting_Data",
    "llm_stage_1_risk_flags": "Initial_Risk_Flags",
    "llm_stage_2_status": "Deep_Review_Status",
    "llm_final_verdict": "Final_Recommendation",
    "llm_final_confidence": "Confidence_Level",
    "llm_final_problem": "Core_Problem",
    "llm_final_cause": "Root_Cause",
    "llm_final_opportunity": "Potential_Gain",
    "llm_final_evidence": "Key_Metrics",
    "llm_final_actions": "Action_Items",
    "llm_final_risk_flags": "Risk_Flags",
}

ESSENTIAL_COLUMNS = [
    "Search_Term",
    "Page_URL",
    "Opportunity_Category",
    "Final_Recommendation",
    "Confidence_Level",
    "Core_Problem",
    "Root_Cause",
    "Potential_Gain",
    "Action_Items",
    "Current_Clicks",
    "Missing_Clicks_Per_Month",
    "Monthly_Search_Volume",
    "Average_Rank_Position",
    "Google_Features_Present",
    "Competition_Level",
    "Risk_Flags",
]


def export_readable_versions(df, output_dir):
    """Export 3 versions: full technical, full readable, essential only"""
    
    # 1. Full technical (original column names, for debugging)
    full_tech_path = os.path.join(output_dir, "final_output_full_technical.csv")
    df.to_csv(full_tech_path, index=False)
    logger.info(f"Saved full technical output → {full_tech_path}")
    
    # 2. Full readable (all columns with human names)
    df_readable = df.rename(columns=COLUMN_MAPPING)
    full_readable_path = os.path.join(output_dir, "final_output_full_readable.csv")
    df_readable.to_csv(full_readable_path, index=False)
    logger.info(f"Saved full readable output → {full_readable_path}")
    
    # 3. Essential only (what localization team needs)
    available_cols = [c for c in ESSENTIAL_COLUMNS if c in df_readable.columns]
    df_essential = df_readable[available_cols].copy()
    
    # Sort: action first, high confidence first
    if "Final_Recommendation" in df_essential.columns:
        priority_map = {"action": 1, "monitor": 2, "ignore": 3}
        confidence_map = {"high": 1, "medium": 2, "low": 3}
        df_essential["_sort_rec"] = df_essential["Final_Recommendation"].map(priority_map).fillna(99)
        df_essential["_sort_conf"] = df_essential["Confidence_Level"].map(confidence_map).fillna(99)
        df_essential = df_essential.sort_values(["_sort_rec", "_sort_conf"], ascending=[True, True])
        df_essential = df_essential.drop(["_sort_rec", "_sort_conf"], axis=1)
    
    essential_path = os.path.join(output_dir, "final_output_for_team.csv")
    df_essential.to_csv(essential_path, index=False)
    logger.info(f"Saved essential output → {essential_path}")
    
    return full_tech_path, full_readable_path, essential_path


# -----------------------
# Main runner (V4 - with observability)
# -----------------------
def run_llm_stage_2():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    logger.info("Starting LLM Stage 2 - Deep Dive")
    
    metrics = Stage2Metrics()
    
    in_path = os.path.join(OUTPUT_DIR, "llm_stage_1_output.csv")
    df = pd.read_csv(in_path)
    
    logger.info(f"Loaded {len(df)} rows from {in_path}")

    # Guarantee output columns (structured reasoning + actions)
    for c in [
        "llm_stage_2_status",
        "llm_final_verdict",
        "llm_final_confidence",
        "llm_final_problem",
        "llm_final_cause",
        "llm_final_opportunity",
        "llm_final_evidence",
        "llm_final_reasoning_raw",
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
    logger.info(f"Found {len(stage2)} rows requiring deep dive analysis")
    
    metrics.total_rows = len(stage2)
    
    if len(stage2) == 0:
        logger.warning("No rows to process. Exporting outputs.")
        export_readable_versions(df, OUTPUT_DIR)
        return

    # Progress bar
    with tqdm(total=len(stage2), desc="LLM Stage 2 Progress", unit="row") as pbar:
        for idx, r in stage2.iterrows():
            try:
                user_input = _build_user_input(r)
                
                # Track API call time
                api_start = time.time()
                resp = client.chat.completions.create(
                    model=MODEL_STAGE_2,
                    messages=[
                        {"role": "system", "content": DEEPDIVE_SYSTEM_PROMPT},
                        {"role": "user", "content": user_input},
                    ],
                    temperature=TEMPERATURE,
                )
                api_time = time.time() - api_start

                raw = resp.choices[0].message.content.strip()
                
                # Extract token usage
                tokens_in = resp.usage.prompt_tokens if resp.usage else 0
                tokens_out = resp.usage.completion_tokens if resp.usage else 0

                try:
                    parsed = json.loads(raw)
                except json.JSONDecodeError:
                    logger.warning(f"Row {idx}: JSON parse failed")
                    metrics.record_error("json_parse")
                    
                    df.loc[idx, "llm_stage_2_status"] = "error"
                    df.loc[idx, "llm_final_verdict"] = "monitor"
                    df.loc[idx, "llm_final_confidence"] = "low"
                    df.loc[idx, "llm_final_reasoning_raw"] = raw[:1000]
                    df.loc[idx, "llm_final_risk_flags"] = "json_parse_failed"
                    time.sleep(SLEEP)
                    pbar.update(1)
                    continue

                verdict = parsed.get("verdict", "monitor")
                confidence = parsed.get("confidence", "low")
                reasoning = parsed.get("reasoning", {})
                actions = parsed.get("recommended_actions", [])
                risk_flags = parsed.get("risk_flags", [])

                # Parse structured reasoning
                if isinstance(reasoning, dict):
                    problem = reasoning.get("problem", "")
                    cause = reasoning.get("cause", "")
                    opportunity = reasoning.get("opportunity", "")
                    evidence = reasoning.get("evidence", "")
                elif isinstance(reasoning, str):
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

                # Enforce risk flags
                if r.get("serp_status") != "ok":
                    risk_flags = list(set(risk_flags + ["serp_unavailable"]))

                if r.get("llm_stage_1_confidence") == "low":
                    risk_flags = list(set(risk_flags + ["low_upstream_confidence"]))

                # Validate actions specificity
                has_generic_actions = False
                generic_phrases = ["improve content", "optimize title", "build links", "add keywords"]
                if actions:
                    for action in actions:
                        if any(phrase in action.lower() for phrase in generic_phrases):
                            if len(action.split()) < 8:
                                has_generic_actions = True
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
                
                # Record success metrics
                metrics.record_success(verdict, confidence, tokens_in, tokens_out, api_time, has_generic_actions)
                
                # Log every 50 rows (stage 2 is slower)
                if metrics.processed_rows % 50 == 0:
                    logger.info(f"Progress: {metrics.processed_rows}/{metrics.total_rows} | "
                              f"Success rate: {(metrics.success_count/metrics.processed_rows)*100:.1f}% | "
                              f"Avg API time: {metrics.avg_api_time:.2f}s")

                time.sleep(SLEEP)
                pbar.update(1)

            except Exception as e:
                logger.error(f"Row {idx}: API error - {str(e)}")
                metrics.record_error("api")
                
                df.loc[idx, "llm_stage_2_status"] = "error"
                df.loc[idx, "llm_final_verdict"] = "monitor"
                df.loc[idx, "llm_final_confidence"] = "low"
                df.loc[idx, "llm_final_reasoning_raw"] = f"llm_error:{str(e)}"
                df.loc[idx, "llm_final_risk_flags"] = "llm_error"
                
                pbar.update(1)

    # Export 3 versions
    tech_path, readable_path, team_path = export_readable_versions(df, OUTPUT_DIR)
    
    # Log and save metrics
    summary = metrics.log_summary()
    
    metrics_path = os.path.join(OUTPUT_DIR, "llm_stage_2_metrics.json")
    with open(metrics_path, 'w') as f:
        json.dump(summary, f, indent=2)
    logger.info(f"Metrics saved → {metrics_path}")
    
    print(f"\n✓ LLM Stage 2 completed")
    print(f"  Processed: {metrics.processed_rows}/{metrics.total_rows}")
    print(f"  Success: {metrics.success_count} ({summary['success_rate']})")
    print(f"  Errors: {metrics.error_count}")
    print(f"  Generic actions flagged: {metrics.generic_action_count}")
    print(f"  Cost: {summary['total_cost_usd']}")
    print(f"  Time: {summary['total_time_seconds']}s")
    print(f"\n💡 Localization team file: {team_path}")
