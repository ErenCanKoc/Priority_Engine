# src/engine_core.py
import os
import pandas as pd
import numpy as np
from config import INPUT_FILE, OUTPUT_DIR, PERIOD_MONTHS, ACTION_PERCENTILE


# -----------------------
# Parsing helpers
# -----------------------
def _parse_number(x):
    if pd.isna(x):
        return np.nan
    s = str(x).replace("%", "").strip()
    # handle "1.234,56" vs "1234.56"
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    else:
        s = s.replace(",", ".")
    return pd.to_numeric(s, errors="coerce")


def _parse_pct(x):
    v = _parse_number(x)
    if pd.isna(v):
        return np.nan
    # if looks like percent (e.g. 177.84), convert to ratio
    if abs(v) > 5:
        return v / 100
    return v


def _expected_ctr(pos: float) -> float:
    if pd.isna(pos):
        return np.nan
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


# -----------------------
# Core Engine (V2 Contract)
# -----------------------
def run_engine():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    df = pd.read_csv(INPUT_FILE, dtype=str)
    df.columns = df.columns.str.lower().str.strip()

    # Normalize Looker/GSC export headers -> engine canonical columns
    df = df.rename(columns={
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
    })

    # Ensure expected columns exist
    expected_columns = [
        "keyword", "url",
        "clicks_last", "clicks_pct",
        "impr_last", "impr_pct",
        "ctr_last", "ctr_pct",
        "pos", "pos_pct",
    ]
    for c in expected_columns:
        if c not in df.columns:
            df[c] = np.nan

    # Parse numeric fields
    for c in ["clicks_last", "impr_last", "pos", "ctr_last"]:
        df[c] = df[c].apply(_parse_number)

    for c in ["clicks_pct", "impr_pct", "ctr_pct", "pos_pct"]:
        df[c] = df[c].apply(_parse_pct)

    # -----------------------
    # Data quality (explicit)
    # -----------------------
    def data_quality(r):
        issues = []
        if pd.isna(r["keyword"]) or str(r["keyword"]).strip() == "":
            issues.append("missing_keyword")
        if pd.isna(r["url"]) or str(r["url"]).strip() == "":
            issues.append("missing_url")
        if pd.isna(r["impr_last"]) or r["impr_last"] <= 0:
            issues.append("missing_impressions")
        if pd.isna(r["pos"]):
            issues.append("missing_position")
        # pct fields are optional; don't block engine hard, just flag
        if pd.isna(r["clicks_pct"]): issues.append("missing_clicks_pct")
        if pd.isna(r["impr_pct"]): issues.append("missing_impr_pct")
        return pd.Series({
            "data_ok": len([i for i in issues if i in ["missing_keyword","missing_url","missing_impressions","missing_position"]]) == 0,
            "data_issues": ",".join(issues),
        })

    df[["data_ok", "data_issues"]] = df.apply(data_quality, axis=1)

    # Prev values (only if pct available)
    df["clicks_prev"] = df.apply(lambda r: _infer_prev(r["clicks_last"], r["clicks_pct"]), axis=1)
    df["impr_prev"] = df.apply(lambda r: _infer_prev(r["impr_last"], r["impr_pct"]), axis=1)

    # Types
    df["page_type"] = df["url"].apply(_page_type)
    df["query_type"] = df["keyword"].apply(_query_type)

    # MSV estimate + utilization (guarded)
    df["msv_est"] = ((df["impr_last"].fillna(0) + df["impr_prev"].fillna(0)) / 2) / max(PERIOD_MONTHS, 1)
    df["utilization"] = df["clicks_last"] / df["msv_est"].replace(0, np.nan)

    # CTR gap -> traffic gap
    df["expected_clicks"] = df["impr_last"] * df["pos"].apply(_expected_ctr)
    df["traffic_gap"] = df["expected_clicks"] - df["clicks_last"]

    # Drops/gains (only meaningful if prev exists)
    df["clicks_drop"] = (df["clicks_prev"] - df["clicks_last"]).clip(lower=0)
    df["clicks_gain"] = (df["clicks_last"] - df["clicks_prev"]).clip(lower=0)

    # Scores
    df["rescue_raw"] = df["traffic_gap"].fillna(0) * np.sqrt(df["clicks_drop"].fillna(0))
    df["scale_raw"] = df["traffic_gap"].fillna(0) * np.sqrt(df["clicks_gain"].fillna(0))

    # rank(pct=True) returns NaN if all equal; guard with fill
    df["rescue_score"] = df["rescue_raw"].rank(pct=True).fillna(0) * 100
    df["scale_score"] = df["scale_raw"].rank(pct=True).fillna(0) * 100

    # Problem type
    def problem_type(r):
        if not r["data_ok"]:
            return "no_data"
        # if we don't have prev, don't claim drop/gain; mark as "insufficient_signals"
        if pd.isna(r["clicks_prev"]) or pd.isna(r["impr_prev"]):
            return "insufficient_signals"
        if (r["clicks_drop"] > 0) and (r["traffic_gap"] > 0):
            return "ctr_or_rank_drop"
        if r["impr_last"] < r["impr_prev"]:
            return "demand_drop"
        if r["clicks_gain"] > 0:
            return "growing"
        return "stable"

    df["problem_type"] = df.apply(problem_type, axis=1)

    # -----------------------
    # V2 DECISION CONTRACT
    # -----------------------
    df["engine_status"] = "ok"
    df.loc[df["problem_type"] == "no_data", "engine_status"] = "no_data"
    df.loc[df["problem_type"] == "insufficient_signals", "engine_status"] = "insufficient_signals"

    df["analyze_candidate"] = False
    df["candidate_type"] = "monitor"
    df["candidate_reason"] = "default_monitor"

    # Hard excludes (explicit)
    hard_exclude = (
        (df["data_ok"] != True) |
        (df["page_type"] == "system") |
        (df["query_type"] == "brand")
    )
    df.loc[hard_exclude, "candidate_type"] = "ignore"
    df.loc[hard_exclude, "candidate_reason"] = "brand/system_or_bad_data"
    df.loc[hard_exclude, "analyze_candidate"] = False

    # For remaining rows, compute candidate logic
    eligible = ~hard_exclude

    # Thresholds you can tune
    MIN_MSV_FOR_ACTION = 300
    MIN_GAP_FOR_ACTION = 30  # clicks opportunity
    PCTL = float(ACTION_PERCENTILE)

    # Rescue: high relative drop + meaningful opportunity
    rescue_mask = (
        eligible &
        (df["rescue_score"] >= PCTL) &
        (df["msv_est"] >= MIN_MSV_FOR_ACTION) &
        (df["traffic_gap"] >= MIN_GAP_FOR_ACTION)
    )
    df.loc[rescue_mask, "candidate_type"] = "rescue"
    df.loc[rescue_mask, "candidate_reason"] = "high_drop_high_potential_gap"
    df.loc[rescue_mask, "analyze_candidate"] = True

    # Scale: momentum + headroom
    scale_mask = (
        eligible &
        (df["scale_score"] >= PCTL) &
        (df["msv_est"] >= MIN_MSV_FOR_ACTION) &
        (pd.notna(df["utilization"])) &
        (df["utilization"] < 0.85) &
        (df["traffic_gap"] >= MIN_GAP_FOR_ACTION)
    )
    df.loc[scale_mask, "candidate_type"] = "scale"
    df.loc[scale_mask, "candidate_reason"] = "momentum_with_headroom"
    df.loc[scale_mask, "analyze_candidate"] = True

    # Expand: growing + far from potential (more conservative)
    expand_mask = (
        eligible &
        (df["problem_type"] == "growing") &
        (df["msv_est"] >= 500) &
        (pd.notna(df["utilization"])) &
        (df["utilization"] < 0.50)
    )
    df.loc[expand_mask, "candidate_type"] = "expand"
    df.loc[expand_mask, "candidate_reason"] = "growing_far_from_potential"
    df.loc[expand_mask, "analyze_candidate"] = True

    # If insufficient signals (no prev) but looks promising, allow SERP/LLM triage
    promising_no_prev = (
        eligible &
        (df["engine_status"] == "insufficient_signals") &
        (df["msv_est"] >= 500) &
        (df["traffic_gap"] >= MIN_GAP_FOR_ACTION)
    )
    df.loc[promising_no_prev, "candidate_type"] = "monitor"
    df.loc[promising_no_prev, "candidate_reason"] = "no_prev_but_high_gap_high_msv"
    df.loc[promising_no_prev, "analyze_candidate"] = True

    out_path = os.path.join(OUTPUT_DIR, "engine_output.csv")
    df.to_csv(out_path, index=False)
    print(f"✔ Engine V2 output saved → {out_path}")
