# src/interpretation_layer_llm.py
"""
LLM-based interpretation layer (NICE TO HAVE #8).

Replaces rule-based classification with LLM-powered analysis.
More robust, handles edge cases, adapts to language variations.
"""

import os
import json
import pandas as pd
from openai import OpenAI
from config import OUTPUT_DIR, OPENAI_API_KEY

client = OpenAI(api_key=OPENAI_API_KEY)

INTERPRETATION_SYSTEM_PROMPT = """
You are an SEO operations specialist categorizing action items for a content team.

Given an SEO recommendation, classify it into:

ACTION TYPES:
1. "ctr_optimization" - Title/meta description changes to improve clicks
2. "snippet_capture" - Optimize to capture featured snippet/answer box
3. "faq_expansion" - Add FAQ section to capture People Also Ask
4. "content_expansion" - Add new sections/topics to existing content
5. "content_consolidation" - Merge/redirect cannibalizing pages
6. "internal_linking" - Add strategic internal links
7. "technical_fix" - Technical SEO issue (rare, flag for SEO team)
8. "general_optimization" - Multiple improvements, not clearly categorized

EFFORT LEVELS:
- "low" (< 2 hours): Title/meta changes, adding FAQ, small text edits
- "medium" (2-8 hours): Content expansion (300-800 words), internal linking, restructuring
- "high" (8+ hours): Major rewrites, new sections (1000+ words), technical changes

OWNER:
- "content_team": Content creation, editing, FAQ, expansion
- "seo_team": Technical fixes, schema, redirect setup
- "seo_content": Mixed (title optimization, snippet capture)

PRIORITY (based on impact + effort):
- "high": High impact (>200 clicks) + low/medium effort
- "medium": Medium impact (50-200 clicks) OR high impact + high effort
- "low": Low impact (<50 clicks) OR very uncertain

Return ONLY valid JSON:
{
  "action_type": "one of the types above",
  "effort": "low" | "medium" | "high",
  "owner": "content_team" | "seo_team" | "seo_content",
  "priority": "high" | "medium" | "low",
  "reasoning": "Brief explanation (1-2 sentences)"
}
""".strip()


def classify_action_with_llm(row: dict) -> dict:
    """
    Use LLM to classify a single action item.
    
    Args:
        row: Dict with keys: llm_final_actions, serp_features, 
             traffic_gap, candidate_type, llm_final_problem
    
    Returns:
        Dict with: action_type, effort, owner, priority, reasoning
    """
    # Build context for LLM
    user_input = f"""
Candidate Type: {row.get('candidate_type', '')}
Problem: {row.get('llm_final_problem', '')}
Traffic Gap: {row.get('traffic_gap', 0)} clicks/month
SERP Features: {row.get('serp_features', 'none')}

Recommended Actions:
{row.get('llm_final_actions', 'No specific actions')}

Classify this action item.
""".strip()
    
    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": INTERPRETATION_SYSTEM_PROMPT},
                {"role": "user", "content": user_input},
            ],
            temperature=0.1,  # Low temp for consistent classification
        )
        
        raw = response.choices[0].message.content.strip()
        parsed = json.loads(raw)
        
        return {
            "action_type": parsed.get("action_type", "general_optimization"),
            "effort": parsed.get("effort", "medium"),
            "owner": parsed.get("owner", "content_team"),
            "priority": parsed.get("priority", "medium"),
            "classification_reasoning": parsed.get("reasoning", ""),
        }
        
    except Exception as e:
        # Fallback to safe defaults
        return {
            "action_type": "general_optimization",
            "effort": "medium",
            "owner": "content_team",
            "priority": "medium",
            "classification_reasoning": f"Classification error: {str(e)}",
        }


def run_llm_interpretation():
    """
    Run LLM-based interpretation on all actionable rows.
    """
    in_path = os.path.join(OUTPUT_DIR, "final_output_for_team.csv")
    out_path = os.path.join(OUTPUT_DIR, "action_output_llm.csv")
    
    if not os.path.exists(in_path):
        print(f"❌ Error: {in_path} not found")
        return
    
    df = pd.read_csv(in_path)
    
    # Detect column names (readable vs technical)
    verdict_col = "Final_Recommendation" if "Final_Recommendation" in df.columns else "llm_final_verdict"
    confidence_col = "Confidence_Level" if "Confidence_Level" in df.columns else "llm_final_confidence"
    problem_col = "Core_Problem" if "Core_Problem" in df.columns else "llm_final_problem"
    actions_col = "Action_Items" if "Action_Items" in df.columns else "llm_final_actions"
    gap_col = "Missing_Clicks_Per_Month" if "Missing_Clicks_Per_Month" in df.columns else "traffic_gap"
    candidate_col = "Opportunity_Category" if "Opportunity_Category" in df.columns else "candidate_type"
    keyword_col = "Search_Term" if "Search_Term" in df.columns else "keyword"
    url_col = "Page_URL" if "Page_URL" in df.columns else "url"
    serp_col = "Google_Features_Present" if "Google_Features_Present" in df.columns else "serp_features"
    
    # Only actionable rows
    if verdict_col not in df.columns:
        print(f"⚠️ Column '{verdict_col}' not found. Skipping interpretation.")
        return
        
    actionable = df[df[verdict_col] == "action"].copy()
    
    if actionable.empty:
        print("⚠️ No actionable rows found.")
        return
    
    print(f"🤖 Classifying {len(actionable)} actions with LLM...")
    
    # Classify each action
    classifications = []
    for idx, row in actionable.iterrows():
        # Map to expected keys for classify function
        row_dict = {
            "candidate_type": row.get(candidate_col, ""),
            "llm_final_problem": row.get(problem_col, ""),
            "traffic_gap": row.get(gap_col, 0),
            "serp_features": row.get(serp_col, ""),
            "llm_final_actions": row.get(actions_col, ""),
        }
        classification = classify_action_with_llm(row_dict)
        classifications.append(classification)
        
        # Progress
        if len(classifications) % 10 == 0:
            print(f"   Processed {len(classifications)}/{len(actionable)}...")
    
    # Add classifications to dataframe
    for key in ["action_type", "effort", "owner", "priority", "classification_reasoning"]:
        actionable[key] = [c[key] for c in classifications]
    
    # Estimated impact (from traffic_gap)
    actionable["estimated_impact_clicks"] = pd.to_numeric(actionable[gap_col], errors='coerce').fillna(0).astype(int)
    
    # Confidence adjustment
    actionable["confidence_adjusted"] = actionable.apply(
        lambda r: "needs_review" if r.get(confidence_col, "low") == "low" else "ok",
        axis=1
    )
    
    # Export columns - use readable names if available
    export_cols = [
        url_col,
        keyword_col,
        candidate_col,
        "action_type",
        "priority",
        "estimated_impact_clicks",
        "effort",
        "owner",
        "confidence_adjusted",
        problem_col,
        actions_col,
        "classification_reasoning",
    ]
    
    available_cols = [c for c in export_cols if c in actionable.columns]
    actionable[available_cols].to_csv(out_path, index=False)
    
    print(f"✅ LLM-based interpretation complete → {out_path}")
    print(f"   Action types breakdown:")
    
    type_counts = actionable["action_type"].value_counts()
    for action_type, count in type_counts.items():
        print(f"      {action_type}: {count}")


if __name__ == "__main__":
    run_llm_interpretation()