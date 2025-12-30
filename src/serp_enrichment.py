# src/serp_enrichment.py
import os
import time
import pandas as pd
import numpy as np
from serpapi.google_search import GoogleSearch
from config import OUTPUT_DIR, SERPAPI_KEY, SERP_LIMIT
from observability import log, track_stage, Metrics


# -----------------------
# SERP fetch (DE / Google.de)
# -----------------------
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


def _parse_serp(result: dict, target_url: str):
    # Guard: API error response
    if isinstance(result, dict) and "error" in result:
        raise RuntimeError(f"serpapi_error:{result.get('error')}")

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
        if isinstance(link, str) and isinstance(target_url, str) and target_url in link:
            rank = i
            break

    ads_present = bool(result.get("ads"))
    heavy_features = len(features) >= 2
    competition_level = (
        "high" if ads_present or heavy_features else "medium" if features else "low"
    )

    return {
        "serp_features": ",".join(sorted(features)),
        "serp_rank": rank,
        "serp_competition": competition_level,
    }


# -----------------------
# Main runner (V2 Contract)
# -----------------------
def run_serp():
    with track_stage("serp"):
        metrics = Metrics("serp")

        os.makedirs(OUTPUT_DIR, exist_ok=True)
        in_path = os.path.join(OUTPUT_DIR, "engine_output.csv")
        df = pd.read_csv(in_path)

        # Ensure columns exist (explicit contract)
        for c in [
            "serp_status",
            "serp_features",
            "serp_rank",
            "serp_competition",
            "serp_error",
            "serp_summary",
        ]:
            if c not in df.columns:
                df[c] = ""

        # Default states
        df["serp_status"] = "skipped"
        df["serp_features"] = ""
        df["serp_rank"] = np.nan
        df["serp_competition"] = ""
        df["serp_error"] = ""
        df["serp_summary"] = ""

        # Candidates are defined ONLY by engine contract
        candidates = df[df.get("analyze_candidate", False) == True].copy()

        # Optional cap (cost control)
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
                parsed = _parse_serp(serp, url)

                df.loc[idx, "serp_status"] = "ok"
                df.loc[idx, "serp_features"] = parsed["serp_features"]
                df.loc[idx, "serp_rank"] = parsed["serp_rank"]
                df.loc[idx, "serp_competition"] = parsed["serp_competition"]
                df.loc[idx, "serp_summary"] = (
                    f"features={parsed['serp_features']}; "
                    f"rank={parsed['serp_rank']}; "
                    f"competition={parsed['serp_competition']}"
                )

                metrics.inc_success()
                time.sleep(1.0)

            except Exception as e:
                df.loc[idx, "serp_status"] = "error"
                df.loc[idx, "serp_error"] = str(e)
                df.loc[idx, "serp_summary"] = f"serp_error:{str(e)}"
                metrics.inc_failed()

        # Emit metrics ONCE per stage
        metrics.emit()

        # Convenience boolean for downstream (LLM)
        df["serp_data_available"] = df["serp_status"] == "ok"

        out_path = os.path.join(OUTPUT_DIR, "serp_output.csv")
        df.to_csv(out_path, index=False)
        print(f"✔ SERP V2 output saved → {out_path}")
