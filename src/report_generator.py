# src/report_generator.py
import os
import pandas as pd
from datetime import datetime
from config import OUTPUT_DIR


def generate_html_report():
    """
    Localization team için HTML report oluşturur.
    final_output_for_team.csv ve action_output_llm.csv'yi okur, human-readable report üretir.
    """
    
    # Input files
    final_path = os.path.join(OUTPUT_DIR, "final_output_for_team.csv")
    action_path = os.path.join(OUTPUT_DIR, "action_output_llm.csv")
    
    if not os.path.exists(final_path):
        print(f"❌ Error: {final_path} not found")
        return None
    
    df_final = pd.read_csv(final_path)
    
    # action_output.csv optional (eğer interpretation layer çalıştıysa var)
    df_action = None
    if os.path.exists(action_path):
        df_action = pd.read_csv(action_path)
    
    # --- COLUMN NAME MAPPING ---
    # final_output_for_team.csv uses readable column names
    verdict_col = "Final_Recommendation" if "Final_Recommendation" in df_final.columns else "llm_final_verdict"
    confidence_col = "Confidence_Level" if "Confidence_Level" in df_final.columns else "llm_final_confidence"
    problem_col = "Core_Problem" if "Core_Problem" in df_final.columns else "llm_final_problem"
    cause_col = "Root_Cause" if "Root_Cause" in df_final.columns else "llm_final_cause"
    opportunity_col = "Potential_Gain" if "Potential_Gain" in df_final.columns else "llm_final_opportunity"
    actions_col = "Action_Items" if "Action_Items" in df_final.columns else "llm_final_actions"
    candidate_col = "Opportunity_Category" if "Opportunity_Category" in df_final.columns else "candidate_type"
    keyword_col = "Search_Term" if "Search_Term" in df_final.columns else "keyword"
    url_col = "Page_URL" if "Page_URL" in df_final.columns else "url"
    clicks_col = "Current_Clicks" if "Current_Clicks" in df_final.columns else "clicks_last"
    gap_col = "Missing_Clicks_Per_Month" if "Missing_Clicks_Per_Month" in df_final.columns else "traffic_gap"
    pos_col = "Average_Rank_Position" if "Average_Rank_Position" in df_final.columns else "pos"
    serp_col = "Google_Features_Present" if "Google_Features_Present" in df_final.columns else "serp_features"
    
    # --- EXECUTIVE SUMMARY STATS ---
    total_rows = len(df_final)
    action_rows = len(df_final[df_final[verdict_col] == "action"]) if verdict_col in df_final.columns else 0
    monitor_rows = len(df_final[df_final[verdict_col] == "monitor"]) if verdict_col in df_final.columns else 0
    
    # High confidence actions
    high_conf_actions = 0
    if verdict_col in df_final.columns and confidence_col in df_final.columns:
        high_conf_actions = len(df_final[
            (df_final[verdict_col] == "action") &
            (df_final[confidence_col] == "high")
        ])
    
    # Candidate types breakdown
    rescue_count = len(df_final[df_final[candidate_col] == "rescue"]) if candidate_col in df_final.columns else 0
    scale_count = len(df_final[df_final[candidate_col] == "scale"]) if candidate_col in df_final.columns else 0
    expand_count = len(df_final[df_final[candidate_col] == "expand"]) if candidate_col in df_final.columns else 0
    
    # Estimated total impact
    total_impact = 0
    if verdict_col in df_final.columns and gap_col in df_final.columns:
        impact_df = df_final[df_final[verdict_col] == "action"][gap_col]
        total_impact = impact_df.sum() if not impact_df.empty else 0
    if pd.isna(total_impact):
        total_impact = 0
    
    # --- TOP PRIORITY PAGES ---
    # Action + high/medium confidence, sorted by traffic_gap
    priority_pages = pd.DataFrame()
    if verdict_col in df_final.columns and confidence_col in df_final.columns:
        priority_pages = df_final[
            (df_final[verdict_col] == "action") &
            (df_final[confidence_col].isin(["high", "medium"]))
        ].copy()
    
    if not priority_pages.empty and gap_col in priority_pages.columns:
        priority_pages = priority_pages.sort_values(gap_col, ascending=False).head(15)
    else:
        priority_pages = df_final.head(15).copy()
    
    # --- BUILD HTML ---
    html = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>German SEO Action Plan - {datetime.now().strftime('%B %Y')}</title>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            line-height: 1.6;
            color: #1e293b;
            background: #f8fafc;
            padding: 20px;
        }}
        
        .container {{
            max-width: 1200px;
            margin: 0 auto;
            background: white;
            border-radius: 16px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
            overflow: hidden;
        }}
        
        .header {{
            background: linear-gradient(135deg, #0f172a 0%, #1e293b 100%);
            color: white;
            padding: 50px;
            text-align: center;
        }}
        
        .header h1 {{
            font-size: 2.5em;
            margin-bottom: 10px;
        }}
        
        .header .date {{
            font-size: 1.2em;
            opacity: 0.9;
        }}
        
        .summary {{
            padding: 40px 50px;
            background: linear-gradient(135deg, #eff6ff 0%, #dbeafe 100%);
            border-bottom: 4px solid #3b82f6;
        }}
        
        .summary h2 {{
            font-size: 2em;
            color: #1e40af;
            margin-bottom: 25px;
        }}
        
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin-top: 30px;
        }}
        
        .stat-card {{
            background: white;
            padding: 25px;
            border-radius: 12px;
            text-align: center;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        
        .stat-number {{
            font-size: 3em;
            font-weight: 800;
            color: #0f172a;
            margin-bottom: 10px;
        }}
        
        .stat-number.highlight {{
            color: #ef4444;
        }}
        
        .stat-label {{
            font-size: 1.1em;
            color: #64748b;
            font-weight: 600;
        }}
        
        .content {{
            padding: 50px;
        }}
        
        .section {{
            margin-bottom: 50px;
        }}
        
        .section-title {{
            font-size: 2em;
            color: #0f172a;
            margin-bottom: 30px;
            padding-bottom: 15px;
            border-bottom: 3px solid #e2e8f0;
        }}
        
        .page-card {{
            background: #f8fafc;
            padding: 30px;
            margin-bottom: 25px;
            border-radius: 12px;
            border-left: 6px solid;
        }}
        
        .page-card.priority-high {{
            border-left-color: #ef4444;
            background: linear-gradient(135deg, #fef2f2 0%, #fee2e2 100%);
        }}
        
        .page-card.priority-medium {{
            border-left-color: #f59e0b;
            background: linear-gradient(135deg, #fffbeb 0%, #fef3c7 100%);
        }}
        
        .page-card.priority-low {{
            border-left-color: #10b981;
            background: linear-gradient(135deg, #ecfdf5 0%, #d1fae5 100%);
        }}
        
        .page-header {{
            display: flex;
            justify-content: space-between;
            align-items: flex-start;
            margin-bottom: 20px;
            flex-wrap: wrap;
            gap: 15px;
        }}
        
        .page-title {{
            font-size: 1.3em;
            font-weight: 700;
            color: #0f172a;
            word-break: break-all;
        }}
        
        .priority-badge {{
            padding: 8px 16px;
            border-radius: 20px;
            font-weight: 700;
            font-size: 0.9em;
        }}
        
        .priority-badge.high {{
            background: #ef4444;
            color: white;
        }}
        
        .priority-badge.medium {{
            background: #f59e0b;
            color: white;
        }}
        
        .priority-badge.low {{
            background: #10b981;
            color: white;
        }}
        
        .page-meta {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
            margin: 20px 0;
            padding: 20px;
            background: white;
            border-radius: 8px;
        }}
        
        .meta-item {{
            display: flex;
            flex-direction: column;
        }}
        
        .meta-label {{
            font-size: 0.85em;
            color: #64748b;
            font-weight: 600;
            margin-bottom: 5px;
        }}
        
        .meta-value {{
            font-size: 1.1em;
            color: #0f172a;
            font-weight: 600;
        }}
        
        .problem-section {{
            margin: 20px 0;
        }}
        
        .problem-section h4 {{
            font-size: 1.1em;
            color: #ef4444;
            margin-bottom: 10px;
        }}
        
        .problem-section p {{
            color: #475569;
            line-height: 1.7;
        }}
        
        .actions-section {{
            margin: 20px 0;
        }}
        
        .actions-section h4 {{
            font-size: 1.1em;
            color: #0f172a;
            margin-bottom: 15px;
        }}
        
        .action-list {{
            list-style: none;
            padding: 0;
        }}
        
        .action-list li {{
            padding: 12px;
            padding-left: 35px;
            margin-bottom: 10px;
            background: white;
            border-radius: 8px;
            position: relative;
        }}
        
        .action-list li:before {{
            content: "✓";
            position: absolute;
            left: 12px;
            color: #10b981;
            font-weight: bold;
            font-size: 1.2em;
        }}
        
        .footer {{
            background: #f1f5f9;
            padding: 30px;
            text-align: center;
            color: #64748b;
        }}
        
        @media print {{
            body {{
                background: white;
                padding: 0;
            }}
            
            .container {{
                box-shadow: none;
            }}
        }}
        
        @media (max-width: 768px) {{
            .header, .summary, .content {{
                padding: 30px 20px;
            }}
            
            .stats-grid {{
                grid-template-columns: 1fr;
            }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <!-- HEADER -->
        <div class="header">
            <h1>🇩🇪 German SEO Action Plan</h1>
            <div class="date">{datetime.now().strftime('%B %Y')}</div>
        </div>
        
        <!-- EXECUTIVE SUMMARY -->
        <div class="summary">
            <h2>📊 Executive Summary</h2>
            <p style="font-size: 1.15em; color: #1e40af; line-height: 1.8; margin-bottom: 20px;">
                Analysis of <strong>{total_rows:,}</strong> German pages identified <strong>{action_rows}</strong> pages requiring action this month.
                Focus on <strong>{high_conf_actions}</strong> high-confidence opportunities for maximum impact.
            </p>
            
            <div class="stats-grid">
                <div class="stat-card">
                    <div class="stat-number highlight">{rescue_count}</div>
                    <div class="stat-label">Pages Need Rescue</div>
                </div>
                <div class="stat-card">
                    <div class="stat-number highlight">{scale_count}</div>
                    <div class="stat-label">Pages Ready to Scale</div>
                </div>
                <div class="stat-card">
                    <div class="stat-number">{expand_count}</div>
                    <div class="stat-label">Expansion Opportunities</div>
                </div>
                <div class="stat-card">
                    <div class="stat-number">+{int(total_impact):,}</div>
                    <div class="stat-label">Potential Clicks/Month</div>
                </div>
            </div>
        </div>
        
        <!-- PRIORITY PAGES -->
        <div class="content">
            <div class="section">
                <div class="section-title">🎯 Top Priority Pages (This Month)</div>
    """
    
    # Add each priority page
    for idx, row in priority_pages.iterrows():
        # Determine priority
        traffic_gap = row.get(gap_col, row.get("traffic_gap", 0))
        confidence = row.get(confidence_col, row.get("llm_final_confidence", "low"))
        
        # Convert to numeric if string
        try:
            traffic_gap = float(traffic_gap) if not pd.isna(traffic_gap) else 0
        except:
            traffic_gap = 0
        
        if traffic_gap >= 100 and confidence == "high":
            priority = "high"
            priority_emoji = "🔴"
        elif traffic_gap >= 50 or confidence == "medium":
            priority = "medium"
            priority_emoji = "🟡"
        else:
            priority = "low"
            priority_emoji = "🟢"
        
        # Extract data using dynamic column names
        url = row.get(url_col, row.get("url", ""))
        keyword = row.get(keyword_col, row.get("keyword", ""))
        candidate_type = row.get(candidate_col, row.get("candidate_type", ""))
        
        problem = row.get(problem_col, row.get("llm_final_problem", "No specific problem identified"))
        cause = row.get(cause_col, row.get("llm_final_cause", "Cause analysis pending"))
        opportunity = row.get(opportunity_col, row.get("llm_final_opportunity", ""))
        
        current_clicks = row.get(clicks_col, row.get("clicks_last", 0))
        missing_clicks = int(traffic_gap) if not pd.isna(traffic_gap) else 0
        position = row.get(pos_col, row.get("pos", 0))
        serp_features = row.get(serp_col, row.get("serp_features", ""))
        
        # Actions
        actions_raw = row.get(actions_col, row.get("llm_final_actions", ""))
        if pd.isna(actions_raw) or actions_raw == "":
            actions = ["Review page content and optimization opportunities"]
        else:
            actions = str(actions_raw).split(" | ")
        
        html += f"""
                <div class="page-card priority-{priority}">
                    <div class="page-header">
                        <div class="page-title">{url}</div>
                        <div class="priority-badge {priority}">{priority_emoji} {priority.upper()} PRIORITY</div>
                    </div>
                    
                    <div style="background: white; padding: 15px; border-radius: 8px; margin-bottom: 20px;">
                        <strong style="color: #0f172a;">🔍 Keyword:</strong> {keyword}
                    </div>
                    
                    <div class="page-meta">
                        <div class="meta-item">
                            <div class="meta-label">Current Clicks</div>
                            <div class="meta-value">{int(current_clicks) if not pd.isna(current_clicks) else 0}/month</div>
                        </div>
                        <div class="meta-item">
                            <div class="meta-label">Missing Clicks</div>
                            <div class="meta-value" style="color: #ef4444;">+{missing_clicks}/month</div>
                        </div>
                        <div class="meta-item">
                            <div class="meta-label">Current Position</div>
                            <div class="meta-value">#{int(position) if not pd.isna(position) else "?"}</div>
                        </div>
                        <div class="meta-item">
                            <div class="meta-label">Type</div>
                            <div class="meta-value">{candidate_type.title()}</div>
                        </div>
                    </div>
                    
                    <div class="problem-section">
                        <h4>❗ What's Happening</h4>
                        <p>{problem}</p>
                    </div>
                    
                    <div class="problem-section">
                        <h4>🔍 Why</h4>
                        <p>{cause}</p>
                    </div>
                    
                    {f'<div class="problem-section"><h4>💡 Opportunity</h4><p>{opportunity}</p></div>' if opportunity else ''}
                    
                    <div class="actions-section">
                        <h4>✅ Action Items</h4>
                        <ul class="action-list">
        """
        
        for action in actions:
            if action.strip():
                html += f"<li>{action.strip()}</li>\n"
        
        html += """
                        </ul>
                    </div>
                </div>
        """
    
    # Close HTML
    html += f"""
            </div>
        </div>
        
        <!-- FOOTER -->
        <div class="footer">
            <p>Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            <p style="margin-top: 10px;">For questions, contact your SEO team</p>
        </div>
    </div>
</body>
</html>
    """
    
    # Save HTML
    output_path = os.path.join(OUTPUT_DIR, "german_seo_action_plan.html")
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)
    
    print(f"✅ Report generated → {output_path}")
    print(f"   📊 {len(priority_pages)} priority pages included")
    print(f"   💡 Open in browser: file://{os.path.abspath(output_path)}")
    
    return output_path


if __name__ == "__main__":
    generate_html_report()