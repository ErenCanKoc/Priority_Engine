# src/main.py
import os
import shutil
import pandas as pd
import numpy as np

from config import OUTPUT_DIR, SERP_LIMIT, LLM_MAX_ITEMS
from engine_core import run_engine_with_cannibalization, _expected_ctr_with_serp
from llm_stage_1_triage import run_llm_stage_1
from llm_stage_1_batch import build_batch_requests
from llm_stage_2_deepdive import run_llm_stage_2
from interpretation_layer import run_llm_interpretation as run_interpretation
from report_generator import generate_html_report

ROW_THRESHOLD = 2000


def run_serp_if_enabled():
    """
    SERP enrichment - sadece SERP_LIMIT > 0 ise çalışır.
    """
    if SERP_LIMIT <= 0:
        print("   ⏭️  SERP devre dışı (SERP_LIMIT=0)")
        print("   📋 Engine output'u SERP output olarak kopyalanıyor...")
        
        # Engine output'u serp_output olarak kopyala
        engine_path = os.path.join(OUTPUT_DIR, "engine_output.csv")
        serp_path = os.path.join(OUTPUT_DIR, "serp_output.csv")
        
        df = pd.read_csv(engine_path)
        
        # SERP kolonlarını boş ekle
        df["serp_status"] = "skipped"
        df["serp_features"] = ""
        df["serp_rank"] = np.nan
        df["serp_competition"] = ""
        df["serp_competition_score"] = 0
        df["serp_dominant_features"] = ""
        df["serp_error"] = ""
        df["serp_summary"] = "serp_disabled"
        df["serp_data_available"] = False
        
        df.to_csv(serp_path, index=False)
        print(f"   ✓ SERP output oluşturuldu (SERP verisi olmadan) → {serp_path}")
        return
    
    # SERP enabled - normal çalıştır
    from serp_enrichment import run_serp_weighted as run_serp
    run_serp()


def update_traffic_gaps_with_serp():
    """
    Recalculate traffic gaps after SERP data is available.
    """
    serp_path = os.path.join(OUTPUT_DIR, "serp_output.csv")
    df = pd.read_csv(serp_path, low_memory=False)
    
    print("   Recalculating traffic gaps with SERP-aware CTR...")
    
    # For rows with SERP data available
    if "serp_data_available" not in df.columns:
        print("   ⚠️ serp_data_available kolonu yok, atlaniyor")
        return
        
    mask = df["serp_data_available"] == True
    rows_with_serp = mask.sum()
    
    if rows_with_serp == 0:
        print("   ⚠️ No SERP data available, skipping CTR adjustment")
        return
    
    # Calculate SERP-adjusted expected clicks
    def calc_serp_adjusted(row):
        if pd.isna(row["pos"]) or pd.isna(row["impr_last"]):
            return np.nan
        ctr = _expected_ctr_with_serp(row["pos"], row.get("serp_features", ""))
        return row["impr_last"] * ctr
    
    df.loc[mask, "expected_clicks_serp_adjusted"] = df[mask].apply(calc_serp_adjusted, axis=1)
    df.loc[mask, "traffic_gap"] = df.loc[mask, "expected_clicks_serp_adjusted"] - df.loc[mask, "clicks_last"]
    
    df.to_csv(serp_path, index=False)
    
    avg_base = df.loc[mask, "expected_clicks_base"].mean() if "expected_clicks_base" in df.columns else 0
    avg_adjusted = df.loc[mask, "expected_clicks_serp_adjusted"].mean()
    reduction_pct = ((avg_base - avg_adjusted) / avg_base * 100) if avg_base > 0 else 0
    
    print(f"   ✓ Updated {rows_with_serp} rows with SERP-aware CTR")
    print(f"   📊 Average expected clicks: {avg_base:.1f} → {avg_adjusted:.1f} ({reduction_pct:.1f}% reduction)")


def run_llm_stage_1_smart():
    """
    Row sayısına göre batch veya realtime LLM çağrısı yapar.
    """
    serp_output_path = os.path.join(OUTPUT_DIR, "serp_output.csv")
    
    if not os.path.exists(serp_output_path):
        raise FileNotFoundError(f"SERP output bulunamadı: {serp_output_path}")
    
    df = pd.read_csv(serp_output_path, low_memory=False)
    candidate_count = len(df[df["analyze_candidate"] == True])
    
    print(f"📊 Toplam satır: {len(df)}, Analiz adayı: {candidate_count}")
    print(f"📊 LLM_MAX_ITEMS: {LLM_MAX_ITEMS}")
    
    # LLM_MAX_ITEMS'a göre cap uygula
    effective_candidates = min(candidate_count, LLM_MAX_ITEMS) if LLM_MAX_ITEMS > 0 else candidate_count
    
    if effective_candidates > ROW_THRESHOLD:
        print(f"⚠️ {effective_candidates} satır > {ROW_THRESHOLD} threshold")
        print("📦 Batch processing öneriliyor (maliyet optimizasyonu için)")
        
        batch_file = build_batch_requests()
        print(f"📄 Batch input hazır: {batch_file}")
        print("💡 OpenAI Batch API'ye manuel upload gerekiyor:")
        print("   https://platform.openai.com/batches")
        print("   Batch tamamlandıktan sonra sonuçları işlemek için ayrı script çalıştırın.")
        
        return "batch_pending"
    else:
        print(f"✅ {effective_candidates} satır <= {ROW_THRESHOLD}, realtime processing başlıyor...")
        run_llm_stage_1()
        return "completed"


def main():
    print("=" * 60)
    print("🚀 SEO PRIORITY ENGINE - PIPELINE START")
    print("=" * 60)
    print(f"   Config: SERP_LIMIT={SERP_LIMIT}, LLM_MAX_ITEMS={LLM_MAX_ITEMS}")
    
    # Ensure output dir exists
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # STEP 1: Engine Core (with Cannibalization Check)
    print("\n▶ STEP 1: Running Priority Engine (with Cannibalization Check)")
    print("-" * 40)
    run_engine_with_cannibalization()

    # STEP 2: SERP Enrichment (opsiyonel)
    print("\n▶ STEP 2: SERP Enrichment")
    print("-" * 40)
    run_serp_if_enabled()

    # STEP 2.5: Update Traffic Gaps with SERP-Aware CTR
    if SERP_LIMIT > 0:
        print("\n▶ STEP 2.5: Update Traffic Gaps with SERP Context")
        print("-" * 40)
        update_traffic_gaps_with_serp()
    else:
        print("\n▶ STEP 2.5: Skipped (SERP disabled)")

    # STEP 3: LLM Stage 1 (Triage)
    print("\n▶ STEP 3: LLM Stage 1 – Triage")
    print("-" * 40)
    stage1_result = run_llm_stage_1_smart()
    
    if stage1_result == "batch_pending":
        print("\n⏸️ Pipeline paused - Batch processing bekliyor")
        print("   Batch tamamlandıktan sonra 'python main.py --resume' çalıştırın")
        return

    # STEP 4: LLM Stage 2 (Deep Dive)
    print("\n▶ STEP 4: LLM Stage 2 – Deep Dive (action only)")
    print("-" * 40)
    run_llm_stage_2()

    # STEP 5: Interpretation Layer
    print("\n▶ STEP 5: Interpretation Layer – Action Planning")
    print("-" * 40)
    run_interpretation()

    # STEP 6: Generate Report
    print("\n▶ STEP 6: Generate HTML Report for Localization Team")
    print("-" * 40)
    report_path = generate_html_report()

    # Summary
    print("\n" + "=" * 60)
    print("✅ PIPELINE COMPLETED SUCCESSFULLY")
    print("=" * 60)
    
    print("\n📊 Output dosyaları:")
    outputs = [
        ("engine_output.csv", "Engine analysis (with cannibalization)"),
        ("serp_output.csv", "SERP data (with weighted competition)"),
        ("llm_stage_1_output.csv", "LLM triage results"),
        ("final_output_full_technical.csv", "Full technical output"),
        ("final_output_full_readable.csv", "Full readable output"),
        ("final_output_for_team.csv", "Team essentials (CSV)"),
        ("action_output_llm.csv", "Action planning (LLM classified)"),
        ("german_seo_action_plan.html", "📄 VISUAL REPORT (for team)"),
    ]
    
    for filename, description in outputs:
        path = os.path.join(OUTPUT_DIR, filename)
        if os.path.exists(path):
            size = os.path.getsize(path) / 1024
            icon = "📄" if filename.endswith(".html") else "✓"
            print(f"   {icon} {filename:40s} ({size:.1f} KB) - {description}")
        else:
            print(f"   ✗ {filename:40s} (not found)")
    
    if report_path and os.path.exists(report_path):
        print("\n" + "=" * 60)
        print("🎉 LOCALIZATION TEAM REPORT READY!")
        print("=" * 60)
        print(f"\n📄 HTML Report: {report_path}")
        print(f"🌐 Open in browser: file://{os.path.abspath(report_path)}")
        print("\n💡 Tip: Open HTML file, then Print → Save as PDF to share")


if __name__ == "__main__":
    main()
