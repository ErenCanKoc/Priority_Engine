# src/serp_enrichment.py
import os
import time
import pandas as pd
import numpy as np
from serpapi import GoogleSearch
from config import OUTPUT_DIR, SERPAPI_KEY, SERP_LIMIT

def _fetch_serp_de(query: str) -> dict:
    params = {
        "q": query,
        "hl": "de",
        "gl": "de",
        "google_domain": "google.de",
        "num": 10,
        "api_key": SERPAPI_KEY
    }
    return GoogleSearch(params).get_dict()

def _parse_serp(result: dict, target_url: str):
    # Features - SerpAPI response fields can vary; these are common
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

    rank = None
    for i, r in enumerate(result.get("organic_results", []), start=1):
        link = r.get("link", "")
        if isinstance(link, str) and target_url in link:
            rank = i
            break

    return sorted(features), rank

def run_serp():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    in_path = os.path.join(OUTPUT_DIR, "engine_output.csv")
    df = pd.read_csv(in_path)

    # placeholders
    df["serp_checked"] = False
    df["serp_features"] = ""
    df["serp_rank"] = np.nan
    df["serp_summary"] = ""

    # pick targets: start with rescue_candidate, highest rescue_score
    candidates = df[df["engine_suggestion"] == "rescue_candidate"].copy()
    if "rescue_score" in candidates.columns:
        candidates = candidates.sort_values("rescue_score", ascending=False)
    candidates = candidates.head(SERP_LIMIT)

    for idx, r in candidates.iterrows():
        try:
            serp = _fetch_serp_de(str(r["keyword"]))
            feats, rank = _parse_serp(serp, str(r["url"]))

            df.loc[idx, "serp_checked"] = True
            df.loc[idx, "serp_features"] = ",".join(feats)
            df.loc[idx, "serp_rank"] = rank
            df.loc[idx, "serp_summary"] = f"features={feats}, rank={rank}"
            time.sleep(1.0)  # be gentle to API
        except Exception as e:
            df.loc[idx, "serp_summary"] = f"serp_error:{str(e)}"

    df["serp_data_available"] = df["serp_checked"] == True

    out_path = os.path.join(OUTPUT_DIR, "serp_output.csv")
    df.to_csv(out_path, index=False)
    print(f"✔ SERP output saved → {out_path}")
