# src/llm_stage_1_triage.py
import os
import json
import time
import logging
import pandas as pd
from datetime import datetime
from openai import OpenAI
from tqdm import tqdm

from config import OUTPUT_DIR, OPENAI_API_KEY, LLM_MAX_ITEMS
from llm_prompts import TRIAGE_SYSTEM_PROMPT
from retry_utils import retry_call, RetryConfig, RetryStats

MODEL_STAGE_1 = "gpt-4o-mini"
TEMPERATURE = 0.2
SLEEP = 0.4

# Retry config: API hataları için daha toleranslı
LLM_RETRY_CONFIG = RetryConfig(
    max_retries=4,
    base_delay=1.5,
    max_delay=90.0,
    exponential_base=2.0,
    jitter=True
)

client = OpenAI(api_key=OPENAI_API_KEY)

# Setup logging
os.makedirs(OUTPUT_DIR, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(OUTPUT_DIR, 'llm_stage_1.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class Stage1Metrics:
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
        
        self.verdict_counts = {"action": 0, "monitor": 0, "ignore": 0}
        self.confidence_counts = {"high": 0, "medium": 0, "low": 0}
        
        self.total_api_time = 0.0
        self.avg_api_time = 0.0
        
        # Retry tracking (YENİ)
        self.retry_stats = RetryStats()
        self.total_retries = 0
        
    def record_success(self, verdict, confidence, tokens_in, tokens_out, api_time, retries=0):
        self.success_count += 1
        self.processed_rows += 1
        self.total_retries += retries
        
        if verdict in self.verdict_counts:
            self.verdict_counts[verdict] += 1
        if confidence in self.confidence_counts:
            self.confidence_counts[confidence] += 1
            
        self.total_tokens_input += tokens_in
        self.total_tokens_output += tokens_out
        
        self.total_cost += (tokens_in * 0.15 / 1_000_000) + (tokens_out * 0.60 / 1_000_000)
        
        self.total_api_time += api_time
        self.avg_api_time = self.total_api_time / self.success_count
        
        self.retry_stats.record_success(retries)
        
    def record_error(self, error_type):
        self.error_count += 1
        self.processed_rows += 1
        
        if error_type == "json_parse":
            self.json_parse_errors += 1
        elif error_type == "api":
            self.api_errors += 1
        
        self.retry_stats.record_failure(error_type)
    
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
            
            # Retry stats (YENİ)
            "total_retries": self.total_retries,
            "avg_retries_per_success": f"{self.total_retries / max(self.success_count, 1):.2f}",
            
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
        logger.info("LLM STAGE 1 METRICS SUMMARY")
        logger.info("=" * 80)
        for key, value in summary.items():
            logger.info(f"{key:30s}: {value}")
        logger.info("=" * 80)
        return summary


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


def _call_llm_with_retry(user_input: str) -> tuple:
    """
    LLM'i retry logic ile çağırır.
    
    Returns:
        (response, retries_used, api_time)
    """
    retries_used = 0
    
    def on_retry(attempt, exception, delay):
        nonlocal retries_used
        retries_used = attempt + 1
        logger.warning(f"LLM retry #{attempt + 1}: {type(exception).__name__}")
    
    api_start = time.time()
    
    resp = retry_call(
        client.chat.completions.create,
        kwargs={
            "model": MODEL_STAGE_1,
            "messages": [
                {"role": "system", "content": TRIAGE_SYSTEM_PROMPT},
                {"role": "user", "content": user_input},
            ],
            "temperature": TEMPERATURE,
        },
        config=LLM_RETRY_CONFIG,
        on_retry=on_retry,
    )
    
    api_time = time.time() - api_start
    return resp, retries_used, api_time


def run_llm_stage_1():
    logger.info("Starting LLM Stage 1 - Triage (with retry)")
    
    metrics = Stage1Metrics()
    
    in_path = os.path.join(OUTPUT_DIR, "serp_output.csv")
    df = pd.read_csv(in_path)
    
    logger.info(f"Loaded {len(df)} rows from {in_path}")

    # Guarantee output columns
    for c in [
        "llm_stage_1_status",
        "llm_stage_1_verdict",
        "llm_stage_1_confidence",
        "llm_stage_1_problem",
        "llm_stage_1_cause",
        "llm_stage_1_opportunity",
        "llm_stage_1_evidence",
        "llm_stage_1_reasoning_raw",
        "llm_stage_1_risk_flags",
        "llm_stage_1_retries",  # YENİ
    ]:
        if c not in df.columns:
            df[c] = ""

    # Defaults
    df["llm_stage_1_status"] = "skipped"
    df["llm_stage_1_verdict"] = "ignore"
    df["llm_stage_1_confidence"] = "low"
    df["llm_stage_1_retries"] = 0

    # Gate: only analyze_candidate=True
    target = df[df.get("analyze_candidate", False) == True].copy()
    logger.info(f"Found {len(target)} candidates for LLM analysis")

    # PRIORITY SORTING: En yüksek potansiyelli sayfaları önce işle
    # 1. rescue_score + scale_score combined
    # 2. traffic_gap (missing clicks)
    # 3. msv_est (search volume)
    if "rescue_score" in target.columns and "scale_score" in target.columns:
        target["_priority_score"] = (
            target["rescue_score"].fillna(0) * 0.4 +
            target["scale_score"].fillna(0) * 0.4 +
            target["traffic_gap"].fillna(0).rank(pct=True) * 100 * 0.2
        )
        target = target.sort_values("_priority_score", ascending=False)
        logger.info("Sorted candidates by priority score (rescue + scale + gap)")
    elif "traffic_gap" in target.columns:
        target = target.sort_values("traffic_gap", ascending=False)
        logger.info("Sorted candidates by traffic_gap")

    if LLM_MAX_ITEMS and LLM_MAX_ITEMS > 0:
        target = target.head(LLM_MAX_ITEMS)
        logger.info(f"Capped to {len(target)} rows (LLM_MAX_ITEMS={LLM_MAX_ITEMS})")
    
    metrics.total_rows = len(target)
    
    if len(target) == 0:
        logger.warning("No rows to process. Exiting.")
        out_path = os.path.join(OUTPUT_DIR, "llm_stage_1_output.csv")
        df.to_csv(out_path, index=False)
        return

    with tqdm(total=len(target), desc="LLM Stage 1 Progress", unit="row") as pbar:
        for idx, r in target.iterrows():
            try:
                user_input = _build_user_input(r)
                
                # Retry ile LLM çağrısı
                resp, retries_used, api_time = _call_llm_with_retry(user_input)

                raw = resp.choices[0].message.content.strip()
                tokens_in = resp.usage.prompt_tokens if resp.usage else 0
                tokens_out = resp.usage.completion_tokens if resp.usage else 0

                try:
                    parsed = json.loads(raw)
                except json.JSONDecodeError:
                    logger.warning(f"Row {idx}: JSON parse failed")
                    metrics.record_error("json_parse")
                    
                    df.loc[idx, "llm_stage_1_status"] = "error"
                    df.loc[idx, "llm_stage_1_verdict"] = "monitor"
                    df.loc[idx, "llm_stage_1_confidence"] = "low"
                    df.loc[idx, "llm_stage_1_reasoning_raw"] = raw[:500]
                    df.loc[idx, "llm_stage_1_risk_flags"] = "json_parse_failed"
                    df.loc[idx, "llm_stage_1_retries"] = retries_used
                    time.sleep(SLEEP)
                    pbar.update(1)
                    continue

                verdict = parsed.get("verdict", "monitor")
                confidence = parsed.get("confidence", "low")
                reasoning = parsed.get("reasoning", {})
                risk_flags = parsed.get("risk_flags", [])

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
                df.loc[idx, "llm_stage_1_retries"] = retries_used
                
                metrics.record_success(verdict, confidence, tokens_in, tokens_out, api_time, retries_used)
                
                if metrics.processed_rows % 100 == 0:
                    logger.info(
                        f"Progress: {metrics.processed_rows}/{metrics.total_rows} | "
                        f"Success: {(metrics.success_count/metrics.processed_rows)*100:.1f}% | "
                        f"Retries: {metrics.total_retries}"
                    )

                time.sleep(SLEEP)
                pbar.update(1)

            except Exception as e:
                # Retry sonrası hala başarısız
                logger.error(f"Row {idx}: Final failure after retries - {str(e)}")
                metrics.record_error("api")
                
                df.loc[idx, "llm_stage_1_status"] = "error"
                df.loc[idx, "llm_stage_1_verdict"] = "monitor"
                df.loc[idx, "llm_stage_1_confidence"] = "low"
                df.loc[idx, "llm_stage_1_reasoning_raw"] = f"llm_error:{str(e)}"
                df.loc[idx, "llm_stage_1_risk_flags"] = "llm_error_after_retries"
                df.loc[idx, "llm_stage_1_retries"] = LLM_RETRY_CONFIG.max_retries
                
                pbar.update(1)

    # Save output
    out_path = os.path.join(OUTPUT_DIR, "llm_stage_1_output.csv")
    df.to_csv(out_path, index=False)
    logger.info(f"Output saved → {out_path}")
    
    summary = metrics.log_summary()
    
    metrics_path = os.path.join(OUTPUT_DIR, "llm_stage_1_metrics.json")
    with open(metrics_path, 'w') as f:
        json.dump(summary, f, indent=2)
    logger.info(f"Metrics saved → {metrics_path}")
    
    print(f"\n✓ LLM Stage 1 completed")
    print(f"  Processed: {metrics.processed_rows}/{metrics.total_rows}")
    print(f"  Success: {metrics.success_count} ({summary['success_rate']})")
    print(f"  Errors: {metrics.error_count}")
    print(f"  Total retries: {metrics.total_retries}")
    print(f"  Cost: {summary['total_cost_usd']}")
    print(f"  Time: {summary['total_time_seconds']}s")
