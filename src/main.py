# src/main.py
import os
import pandas as pd

from config import OUTPUT_DIR
from engine_core import run_engine
from serp_enrichment import run_serp
from llm_stage_1_triage import run_llm_stage_1
from llm_stage_1_batch import build_batch_requests  # Batch input oluşturucu
from llm_stage_2_deepdive import run_llm_stage_2
from interpretation_layer import run_interpretation

ROW_THRESHOLD = 2000


def run_llm_stage_1_smart():
    """
    Row sayısına göre batch veya realtime LLM çağrısı yapar.
    2000+ satır için batch processing önerilir (maliyet optimizasyonu).
    """
    serp_output_path = os.path.join(OUTPUT_DIR, "serp_output.csv")
    
    if not os.path.exists(serp_output_path):
        raise FileNotFoundError(f"SERP output bulunamadı: {serp_output_path}")
    
    df = pd.read_csv(serp_output_path)
    candidate_count = len(df[df["analyze_candidate"] == True])
    
    print(f"📊 Toplam satır: {len(df)}, Analiz adayı: {candidate_count}")
    
    if candidate_count > ROW_THRESHOLD:
        print(f"⚠️  {candidate_count} satır > {ROW_THRESHOLD} threshold")
        print("📦 Batch processing öneriliyor (maliyet optimizasyonu için)")
        
        # Batch input dosyası oluştur
        batch_file = build_batch_requests()
        print(f"📄 Batch input hazır: {batch_file}")
        print("💡 OpenAI Batch API'ye manuel upload gerekiyor:")
        print("   https://platform.openai.com/batches")
        print("   Batch tamamlandıktan sonra sonuçları işlemek için ayrı script çalıştırın.")
        
        return "batch_pending"
    else:
        print(f"✅ {candidate_count} satır <= {ROW_THRESHOLD}, realtime processing başlıyor...")
        run_llm_stage_1()
        return "completed"


def main():
    print("=" * 60)
    print("🚀 SEO PRIORITY ENGINE - PIPELINE START")
    print("=" * 60)
    
    # STEP 1: Engine Core
    print("\n▶ STEP 1: Running Priority Engine")
    print("-" * 40)
    run_engine()

    # STEP 2: SERP Enrichment
    print("\n▶ STEP 2: SERP Enrichment")
    print("-" * 40)
    run_serp()

    # STEP 3: LLM Stage 1 (Triage)
    print("\n▶ STEP 3: LLM Stage 1 – Triage")
    print("-" * 40)
    stage1_result = run_llm_stage_1_smart()
    
    if stage1_result == "batch_pending":
        print("\n⏸️  Pipeline paused - Batch processing bekliyor")
        print("   Batch tamamlandıktan sonra 'python main.py --resume' çalıştırın")
        return

    # STEP 4: LLM Stage 2 (Deep Dive)
    print("\n▶ STEP 4: LLM Stage 2 – Deep Dive (action only)")
    print("-" * 40)
    run_llm_stage_2()

    # STEP 5: Interpretation Layer (YENİ!)
    print("\n▶ STEP 5: Interpretation Layer – Action Planning")
    print("-" * 40)
    run_interpretation()

    # Summary
    print("\n" + "=" * 60)
    print("✅ PIPELINE COMPLETED SUCCESSFULLY")
    print("=" * 60)
    
    print("\n📁 Output dosyaları:")
    outputs = [
        "engine_output.csv",
        "serp_output.csv", 
        "llm_stage_1_output.csv",
        "final_output_full_technical.csv",
        "final_output_full_readable.csv",
        "final_output_for_team.csv",
        "action_output.csv",
    ]
    for f in outputs:
        path = os.path.join(OUTPUT_DIR, f)
        if os.path.exists(path):
            size = os.path.getsize(path) / 1024
            print(f"   ✓ {f} ({size:.1f} KB)")
        else:
            print(f"   ✗ {f} (not found)")


if __name__ == "__main__":
    main()
