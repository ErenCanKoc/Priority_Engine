# src/engine_core.py
import os
import pandas as pd
import numpy as np
from config import INPUT_FILE, OUTPUT_DIR, PERIOD_MONTHS, ACTION_PERCENTILE

def _parse_number(x):
    if pd.isna(x):
        return np.nan
    s = str(x).replace('%', '').strip()
    if ',' in s and '.' in s:
        s = s.replace('.', '').replace(',', '.')
    else:
        s = s.replace(',', '.')
    return pd.to_numeric(s, errors='coerce')

def _parse_pct(x):
    v = _parse_number(x)
    if pd.isna(v):
        return np.nan
    # 177.84 gibi “yüzde” gelirse 1.7784’e çevir
    if abs(v) > 5:
        return v / 100
    return v

def _expected_ctr(pos: float) -> float:
    if pos <= 1: return 0.28
    if pos <= 2: return 0.15
    if pos <= 3: return 0.11
    if pos <= 4: return 0.08
    if pos <= 5: return 0.06
    if pos <= 10: return 0.03
    return 0.01

def _infer_prev(last, pct):
    if pd.isna(last) or pd.isna(pct):
        return np.nan
    return last / (1 + pct)

def _page_type(url):
    if pd.isna(url): return "unknown"
    u = str(url).lower()
    if "/login" in u or "/app" in u or "/user" in u:
        return "system"
    if "/blog/" in u:
        return "blog"
    if "/template" in u:
        return "template"
    if "/features/" in u:
        return "feature"
    return "other"

def _query_type(q):
    if pd.isna(q): return "unknown"
    ql = str(q).lower()
    if "jotform" in ql or "login" in ql or "sign in" in ql:
        return "brand"
    return "non-brand"

def run_engine():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    df = pd.read_csv(INPUT_FILE, dtype=str)
    df.columns = df.columns.str.lower().str.strip()

    # Expect these headers in your Looker export (case-insensitive):
    df = df.rename(
        columns={
            "query": "keyword",
            "landing page": "url",
            "url clicks": "clicks_last",
            "clicks percent change": "clicks_pct",
            "impressions": "impr_last",
            "impression percent change": "impr_pct",
            "url ctr": "ctr_last",
            "ctr percent change": "ctr_pct",
            "avg. position": "pos",
            "avg. position percent change": "pos_pct",
        }
    )

    expected_columns = [
        "keyword",
        "url",
        "clicks_last",
        "clicks_pct",
        "impr_last",
        "impr_pct",
        "ctr_last",
        "ctr_pct",
        "pos",
        "pos_pct",
    ]
    for column in expected_columns:
        if column not in df.columns:
            df[column] = np.nan

    # Parse
    for c in ["clicks_last", "impr_last", "pos", "ctr_last"]:
        if c in df.columns:
            df[c] = df[c].apply(_parse_number)

    for c in ["clicks_pct", "impr_pct", "ctr_pct", "pos_pct"]:
        if c in df.columns:
            df[c] = df[c].apply(_parse_pct)

    # Data quality
    def data_quality(r):
        issues = []
        if pd.isna(r["impr_last"]) or r["impr_last"] <= 0:
            issues.append("missing_impressions")
        if pd.isna(r["pos"]):
            issues.append("missing_position")
        if pd.isna(r["clicks_pct"]):
            issues.append("missing_clicks_pct")
        if pd.isna(r["impr_pct"]):
            issues.append("missing_impr_pct")
        return pd.Series({
            "data_ok": len(issues) == 0,
            "data_issues": ",".join(issues),
        })

    df[["data_ok", "data_issues"]] = df.apply(data_quality, axis=1)

    # Prev values (from % change)
    df["clicks_prev"] = df.apply(lambda r: _infer_prev(r["clicks_last"], r["clicks_pct"]), axis=1)
    df["impr_prev"] = df.apply(lambda r: _infer_prev(r["impr_last"], r["impr_pct"]), axis=1)

    # Types
    df["page_type"] = df["url"].apply(_page_type)
    df["query_type"] = df["keyword"].apply(_query_type)

    # MSV estimate + utilization
    df["msv_est"] = ((df["impr_last"].fillna(0) + df["impr_prev"].fillna(0)) / 2) / PERIOD_MONTHS
    df["utilization"] = df["clicks_last"] / df["msv_est"].replace(0, np.nan)

    # CTR gap
    df["expected_clicks"] = df["impr_last"] * df["pos"].apply(_expected_ctr)
    df["traffic_gap"] = df["expected_clicks"] - df["clicks_last"]

    # Drops/gains
    df["clicks_drop"] = (df["clicks_prev"] - df["clicks_last"]).clip(lower=0)
    df["clicks_gain"] = (df["clicks_last"] - df["clicks_prev"]).clip(lower=0)

    # Scores (stable-ish)
    df["rescue_raw"] = df["traffic_gap"] * np.sqrt(df["clicks_drop"].fillna(0))
    df["scale_raw"] = df["traffic_gap"] * np.sqrt(df["clicks_gain"].fillna(0))
    df["rescue_score"] = df["rescue_raw"].rank(pct=True) * 100
    df["scale_score"] = df["scale_raw"].rank(pct=True) * 100

    # Problem type (simple v1)
    def problem_type(r):
        if not r["data_ok"]:
            return "no_data"
        if (r["clicks_drop"] > 0) and (r["traffic_gap"] > 0):
            return "ctr_or_rank_drop"
        if r["impr_last"] < r["impr_prev"]:
            return "demand_drop"
        if r["clicks_gain"] > 0:
            return "growing"
        return "stable"

    df["problem_type"] = df.apply(problem_type, axis=1)

    # Suggestion (NOT final action)
    def suggest(r):
        if not r["data_ok"]:
            return "ignore", "insufficient data"
        if r["page_type"] == "system" or r["query_type"] == "brand":
            return "monitor", "brand/system page"
        if (r["rescue_score"] >= ACTION_PERCENTILE) and (r["msv_est"] >= 300):
            return "rescue_candidate", "high relative drop on high potential page"
        if (r["scale_score"] >= ACTION_PERCENTILE) and (pd.notna(r["utilization"]) and r["utilization"] < 0.85):
            return "scale_candidate", "positive momentum with headroom"
        # growing + big potential but still low utilization = expand opportunity
        if (r["problem_type"] == "growing") and (r["msv_est"] >= 500) and (pd.notna(r["utilization"]) and r["utilization"] < 0.5):
            return "expand_candidate", "growing but far from potential"
        return "monitor", "no strong signal"

    df[["engine_suggestion", "suggestion_reason"]] = df.apply(lambda r: pd.Series(suggest(r)), axis=1)

    out_path = os.path.join(OUTPUT_DIR, "engine_output.csv")
    df.to_csv(out_path, index=False)
    print(f"✔ Engine output saved → {out_path}")

