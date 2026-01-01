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
    """Base CTR by position (no SERP features)"""
    if pd.isna(pos):
        return np.nan
    if pos <= 1: return 0.28
    if pos <= 2: return 0.15
    if pos <= 3: return 0.11
    if pos <= 4: return 0.08
    if pos <= 5: return 0.06
    if pos <= 10: return 0.03
    return 0.01


def _expected_ctr_with_serp(pos: float, serp_features: str) -> float:
    """
    SERP-aware CTR calculation (IMPORTANT #5 fix).
    
    Accounts for SERP feature impact:
    - Featured Snippet: -40% CTR
    - PAA: -15% CTR
    - Video: -25% CTR
    - Images: -10% CTR
    - Shopping: -20% CTR
    
    Cumulative penalties, capped at -70%.
    """
    base_ctr = _expected_ctr(pos)
    
    if pd.isna(base_ctr) or pd.isna(serp_features) or str(serp_features).strip() == "":
        return base_ctr
    
    features = str(serp_features).lower().split(",")
    features = [f.strip() for f in features]
    
    penalty = 0.0
    if "featured_snippet" in features:
        penalty += 0.40
    if "paa" in features:
        penalty += 0.15
    if "video" in features:
        penalty += 0.25
    if "images" in features:
        penalty += 0.10
    if "shopping" in features:
        penalty += 0.20
    
    penalty = min(penalty, 0.70)  # Cap
    return base_ctr * (1 - penalty)


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
# Core Engine
# -----------------------
def run_engine():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    df = pd.read_csv(INPUT_FILE, dtype=str)
    df.columns = df.columns.str.lower().str.strip()

    # Normalize headers (support both old GSC format and pre-processed format)
    df = df.rename(columns={
        # Old GSC format
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

    # Check if we have pre-processed data (clicks_prev exists) or raw GSC data (clicks_pct exists)
    has_prev_data = "clicks_prev" in df.columns and "impr_prev" in df.columns
    
    print(f"   📊 Input format: {'pre-processed (has _prev columns)' if has_prev_data else 'raw GSC (has _pct columns)'}")

    expected_columns = [
        "keyword", "url", "clicks_last",
        "impr_last", "ctr_last", "pos",
    ]
    for c in expected_columns:
        if c not in df.columns:
            df[c] = np.nan

    # Parse numeric - core columns
    for c in ["clicks_last", "impr_last", "pos", "ctr_last"]:
        if c in df.columns:
            df[c] = df[c].apply(_parse_number)
    
    # Handle previous period data based on input format
    if has_prev_data:
        # Pre-processed format: clicks_prev and impr_prev already exist
        print("   ✓ Using existing clicks_prev/impr_prev columns")
        for c in ["clicks_prev", "impr_prev"]:
            if c in df.columns:
                df[c] = df[c].apply(_parse_number)
    else:
        # Raw GSC format: calculate from pct change
        print("   ✓ Calculating clicks_prev/impr_prev from pct columns")
        for c in ["clicks_pct", "impr_pct", "ctr_pct", "pos_pct"]:
            if c in df.columns:
                df[c] = df[c].apply(_parse_pct)
        df["clicks_prev"] = df.apply(lambda r: _infer_prev(r["clicks_last"], r.get("clicks_pct")), axis=1)
        df["impr_prev"] = df.apply(lambda r: _infer_prev(r["impr_last"], r.get("impr_pct")), axis=1)

    # Data quality
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
        # Check for previous period data (either format)
        if pd.isna(r.get("clicks_prev")) and pd.isna(r.get("clicks_pct")): 
            issues.append("missing_prev_data")
        return pd.Series({
            "data_ok": len([i for i in issues if i in ["missing_keyword","missing_url","missing_impressions","missing_position"]]) == 0,
            "data_issues": ",".join(issues),
        })

    df[["data_ok", "data_issues"]] = df.apply(data_quality, axis=1)

    # Types
    df["page_type"] = df["url"].apply(_page_type)
    df["query_type"] = df["keyword"].apply(_query_type)

    # MSV + utilization
    df["msv_est"] = ((df["impr_last"].fillna(0) + df["impr_prev"].fillna(0)) / 2) / max(PERIOD_MONTHS, 1)
    df["utilization"] = df["clicks_last"] / df["msv_est"].replace(0, np.nan)

    # Expected clicks (base calculation without SERP context)
    df["expected_clicks_base"] = df["impr_last"] * df["pos"].apply(_expected_ctr)
    
    # IMPORTANT #5: Add SERP-aware expected clicks
    # This will be populated AFTER serp_enrichment stage
    df["expected_clicks_serp_adjusted"] = np.nan
    
    # Use base for now (will be recalculated after SERP data available)
    df["expected_clicks"] = df["expected_clicks_base"]
    df["traffic_gap"] = df["expected_clicks"] - df["clicks_last"]

    # Drops/gains
    df["clicks_drop"] = (df["clicks_prev"] - df["clicks_last"]).clip(lower=0)
    df["clicks_gain"] = (df["clicks_last"] - df["clicks_prev"]).clip(lower=0)

    # Scores
    df["rescue_raw"] = df["traffic_gap"].fillna(0) * np.sqrt(df["clicks_drop"].fillna(0))
    df["scale_raw"] = df["traffic_gap"].fillna(0) * np.sqrt(df["clicks_gain"].fillna(0))
    df["rescue_score"] = df["rescue_raw"].rank(pct=True).fillna(0) * 100
    df["scale_score"] = df["scale_raw"].rank(pct=True).fillna(0) * 100

    # Problem type
    def problem_type(r):
        if not r["data_ok"]:
            return "no_data"
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

    # Decision contract
    df["engine_status"] = "ok"
    df.loc[df["problem_type"] == "no_data", "engine_status"] = "no_data"
    df.loc[df["problem_type"] == "insufficient_signals", "engine_status"] = "insufficient_signals"

    df["analyze_candidate"] = False
    df["candidate_type"] = "monitor"
    df["candidate_reason"] = "default_monitor"

    # Hard excludes
    hard_exclude = (
        (df["data_ok"] != True) |
        (df["page_type"] == "system") |
        (df["query_type"] == "brand")
    )
    df.loc[hard_exclude, "candidate_type"] = "ignore"
    df.loc[hard_exclude, "candidate_reason"] = "brand/system_or_bad_data"
    df.loc[hard_exclude, "analyze_candidate"] = False

    eligible = ~hard_exclude
    MIN_MSV_FOR_ACTION = 300
    MIN_GAP_FOR_ACTION = 30
    PCTL = float(ACTION_PERCENTILE)

    # Rescue
    rescue_mask = (
        eligible &
        (df["rescue_score"] >= PCTL) &
        (df["msv_est"] >= MIN_MSV_FOR_ACTION) &
        (df["traffic_gap"] >= MIN_GAP_FOR_ACTION)
    )
    df.loc[rescue_mask, "candidate_type"] = "rescue"
    df.loc[rescue_mask, "candidate_reason"] = "high_drop_high_potential_gap"
    df.loc[rescue_mask, "analyze_candidate"] = True

    # Scale
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

    # Expand
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

    # Promising no prev
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
    print(f"✓ Engine V2 output saved → {out_path}")


# -----------------------
# IMPORTANT #6: Cannibalization Detection
# -----------------------
def detect_cannibalization(df):
    """
    Detects keyword cannibalization: multiple URLs ranking for same keyword.
    
    Adds columns:
    - cannibalization_risk: boolean (True if multiple URLs for same keyword)
    - cannibalization_group: group ID for cannibalizing pages
    - cannibalization_urls: list of URLs competing for same keyword
    """
    # Count URLs per keyword
    keyword_counts = df.groupby("keyword")["url"].nunique()
    cannibal_keywords = keyword_counts[keyword_counts > 1].index.tolist()
    
    # Mark rows
    df["cannibalization_risk"] = df["keyword"].isin(cannibal_keywords)
    
    # Assign group IDs
    df["cannibalization_group"] = ""
    for keyword in cannibal_keywords:
        mask = df["keyword"] == keyword
        # Use keyword as group ID
        df.loc[mask, "cannibalization_group"] = f"cannibal_{hash(keyword) % 10000}"
        
        # List all competing URLs
        urls = df.loc[mask, "url"].tolist()
        urls_str = " | ".join(urls)
        df.loc[mask, "cannibalization_urls"] = urls_str
    
    # Add to analyze candidates if not already
    # Cannibalization is always worth investigating
    cannibal_mask = df["cannibalization_risk"] == True
    df.loc[cannibal_mask, "analyze_candidate"] = True
    
    # Update candidate reason
    df.loc[cannibal_mask & (df["candidate_reason"] == "default_monitor"), 
           "candidate_reason"] = "cannibalization_detected"
    
    cannibal_count = len(df[df["cannibalization_risk"] == True])
    print(f"   ⚠️  Detected {cannibal_count} rows with cannibalization risk")
    
    return df


# Update run_engine to include cannibalization check
def run_engine_with_cannibalization():
    """
    Enhanced engine with cannibalization detection.
    Call this instead of run_engine() to enable IMPORTANT #6 fix.
    """
    # Run normal engine
    run_engine()
    
    # Load output
    out_path = os.path.join(OUTPUT_DIR, "engine_output.csv")
    df = pd.read_csv(out_path)
    
    # Add cannibalization detection
    df = detect_cannibalization(df)
    
    # Save enhanced output
    df.to_csv(out_path, index=False)
    print(f"✓ Engine V3 (with cannibalization check) → {out_path}")
