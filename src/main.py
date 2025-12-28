from engine_core import run_engine
from serp_enrichment import run_serp
from llm_stage_1_triage import run_llm_stage_1
from llm_stage_2_deepdive import run_llm_stage_2

def main():
    print("▶ STEP 1: Running Priority Engine")
    run_engine()

    print("▶ STEP 2: SERP Enrichment")
    run_serp()

    print("▶ STEP 3: LLM Stage 1 – Triage (all rows)")
    run_llm_stage_1()

    print("▶ STEP 4: LLM Stage 2 – Deep Dive (action only)")
    run_llm_stage_2()

    print("✅ Pipeline completed successfully")

if __name__ == "__main__":
    main()