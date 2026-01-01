# src/serp_enrichment_weighted.py
"""
Enhanced SERP enrichment with weighted competition scoring (NICE TO HAVE #9).

Instead of simple high/medium/low, calculates numeric competition score
based on research-backed impact of each SERP feature.
"""

import os
import time
import pandas as pd
import numpy as np
from serpapi.google_search import GoogleSearch
from config import OUTPUT_DIR, SERPAPI_KEY, SERP_LIMIT
from observability import log, track_stage, Metrics


# -----------------------
# SERP Feature Weights (Research-Backed)
# -----------------------
SERP_FEATURE_WEIGHTS = {
    "featured_snippet": 40,     # Steals most clicks (position 0)
    "paa": 15,                   # Inline answers reduce clicks
    "video": 25,                 # Visual distraction, high engagement
    "images": 10,                # Visual browsing
    "top_stories": 20,           # News pack steals clicks
    "shopping": 20,              # E-commerce intent
    "local_pack": 30,            # Local results dominate
    "knowledge_panel": 25,       # Entity information
}

ADS_WEIGHT = 20  # Ads present adds this score


def calculate_competition_score(result: dict) -> dict:
    """
    Calculate weighted competition score based on SERP features.
    
    Returns:
        {
            "competition_score": 0-100 (numeric),
            "competition_level": "low" | "medium" | "high" | "extreme",
            "dominant_features": ["list", "of", "features"],
        }
    
    Scoring:
      0-20: Low competition (clean SERP)
      21-40: Medium competition (1-2 features)
      41-70: High competition (multiple features)
      71-100: Extreme competition (feature-heavy + ads)
    """
    features = {}
    
    # Detect features
    if result.get("featured_snippet"):
        features["featured_snippet"] = SERP_FEATURE_WEIGHTS["featured_snippet"]
    if result.get("people_also_ask"):
        features["paa"] = SERP_FEATURE_WEIGHTS["paa"]
    if result.get("video_results"):
        features["video"] = SERP_FEATURE_WEIGHTS["video"]
    if result.get("image_results"):
        features["images"] = SERP_FEATURE_WEIGHTS["images"]
    if result.get("top_stories"):
        features["top_stories"] = SERP_FEATURE_WEIGHTS["top_stories"]
    if result.get("shopping_results"):
        features["shopping"] = SERP_FEATURE_WEIGHTS["shopping"]
    if result.get("local_results"):
        features["local_pack"] = SERP_FEATURE_WEIGHTS["local_pack"]
    if result.get("knowledge_graph"):
        features["knowledge_panel"] = SERP_FEATURE_WEIGHTS["knowledge_panel"]
    
    # Ads
    ads_present = bool(result.get("ads"))
    if ads_present:
        features["ads"] = ADS_WEIGHT
    
    # Calculate total score
    competition_score = sum(features.values())
    
    # Cap at 100
    competition_score = min(competition_score, 100)
    
    # Categorize
    if competition_score <= 20:
        level = "low"
    elif competition_score <= 40:
        level = "medium"
    elif competition_score <= 70:
        level = "high"
    else:
        level = "extreme"
    
    # Identify dominant features (weight >= 20)
    dominant = [name for name, weight in features.items() if weight >= 20]
    
    return {
        "competition_score": competition_score,
        "competition_level": level,
        "dominant_features": ",".join(dominant) if dominant else "none",
    }


def _fetch_serp_de(query: str) -> dict:
    if not SERPAPI_KEY:
        raise RuntimeError("missing_serpapi_key")

    params = {
        "engine": "google",
        "q": query,
        "hl": "de",
        "gl": "de",
        "google_domain": "google.de",
        "num": 10,
        "api_key": SERPAPI_KEY,
    }
    return GoogleSearch(params).get_dict()


def _parse_serp_weighted(result: dict, target_url: str):
    """Enhanced parser with weighted competition scoring."""
    
    # Guard: API error
    if isinstance(result, dict) and "error" in result:
        raise RuntimeError(f"serpapi_error:{result.get('error')}")

    # Collect features (for compatibility)
    features = set()
    if result.get("featured_snippet"):
        features.add("featured_snippet")
    if result.get("people_also_ask"):
        features.add("paa")
    if result.get("video_results"):
        features.add("video")
    if result.get("image_results"):
        features.add("images")
    if result.get("top_stories"):
        features.add("top_stories")
    if result.get("shopping_results"):
        features.add("shopping")
    if result.get("local_results"):
        features.add("local_pack")
    if result.get("knowledge_graph"):
        features.add("knowledge_panel")

    # Find our rank
    rank = None
    for i, r in enumerate(result.get("organic_results", []), start=1):
        link = r.get("link", "")
        if isinstance(link, str) and isinstance(target_url, str) and target_url in link:
            rank = i
            break

    # Weighted competition scoring
    competition = calculate_competition_score(result)

    return {
        "serp_features": ",".join(sorted(features)),
        "serp_rank": rank,
        "serp_competition": competition["competition_level"],
        "serp_competition_score": competition["competition_score"],
        "serp_dominant_features": competition["dominant_features"],
    }


def run_serp_weighted():
    """
    Enhanced SERP enrichment with weighted competition scoring.
    """
    with track_stage("serp_weighted"):
        metrics = Metrics("serp_weighted")

        os.makedirs(OUTPUT_DIR, exist_ok=True)
        in_path = os.path.join(OUTPUT_DIR, "engine_output.csv")
        df = pd.read_csv(in_path)

        # Ensure columns
        for c in [
            "serp_status",
            "serp_features",
            "serp_rank",
            "serp_competition",
            "serp_competition_score",
            "serp_dominant_features",
            "serp_error",
            "serp_summary",
        ]:
            if c not in df.columns:
                df[c] = ""

        # Defaults
        df["serp_status"] = "skipped"
        df["serp_features"] = ""
        df["serp_rank"] = np.nan
        df["serp_competition"] = ""
        df["serp_competition_score"] = 0
        df["serp_dominant_features"] = ""
        df["serp_error"] = ""
        df["serp_summary"] = ""

        # Candidates
        candidates = df[df.get("analyze_candidate", False) == True].copy()

        # Cap
        if SERP_LIMIT and SERP_LIMIT > 0:
            candidates = candidates.head(SERP_LIMIT)

        for idx, r in candidates.iterrows():
            metrics.inc_total()
            try:
                query = str(r.get("keyword", "")).strip()
                url = str(r.get("url", "")).strip()

                if not query:
                    raise RuntimeError("empty_query")

                serp = _fetch_serp_de(query)
                parsed = _parse_serp_weighted(serp, url)

                df.loc[idx, "serp_status"] = "ok"
                df.loc[idx, "serp_features"] = parsed["serp_features"]
                df.loc[idx, "serp_rank"] = parsed["serp_rank"]
                df.loc[idx, "serp_competition"] = parsed["serp_competition"]
                df.loc[idx, "serp_competition_score"] = parsed["serp_competition_score"]
                df.loc[idx, "serp_dominant_features"] = parsed["serp_dominant_features"]
                df.loc[idx, "serp_summary"] = (
                    f"features={parsed['serp_features']}; "
                    f"rank={parsed['serp_rank']}; "
                    f"competition={parsed['serp_competition']} ({parsed['serp_competition_score']})"
                )

                metrics.inc_success()
                time.sleep(1.0)

            except Exception as e:
                df.loc[idx, "serp_status"] = "error"
                df.loc[idx, "serp_error"] = str(e)
                df.loc[idx, "serp_summary"] = f"serp_error:{str(e)}"
                metrics.inc_failed()

        # Emit metrics
        metrics.emit()

        df["serp_data_available"] = df["serp_status"] == "ok"

        out_path = os.path.join(OUTPUT_DIR, "serp_output.csv")
        df.to_csv(out_path, index=False)
        print(f"✓ SERP V3 (weighted) output saved → {out_path}")
        
        # Competition distribution stats
        if len(df[df["serp_data_available"] == True]) > 0:
            print(f"\n   Competition Score Distribution:")
            comp_scores = df[df["serp_data_available"] == True]["serp_competition_score"]
            print(f"      Mean: {comp_scores.mean():.1f}")
            print(f"      Median: {comp_scores.median():.1f}")
            print(f"      Max: {comp_scores.max():.0f}")
            
            comp_levels = df[df["serp_data_available"] == True]["serp_competition"].value_counts()
            for level, count in comp_levels.items():
                print(f"      {level}: {count}")


if __name__ == "__main__":
    run_serp_weighted()
