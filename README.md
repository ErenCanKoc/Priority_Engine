# Priority Engine – SERP + LLM

Priority Engine is an SEO analysis pipeline that blends GSC-style performance data with optional SERP enrichment and two LLM stages. It produces prioritized action recommendations and a shareable HTML report for localization/content teams.

## What it does
- **Scores and prioritizes** pages for rescue/scale/expand opportunities.
- **Enriches with SERP features** (optional) to adjust CTR expectations.
- **Uses LLM triage + deep dive** to generate actionable recommendations.
- **Generates a final report** in CSV and HTML formats.

## Quick start
```bash
git clone <repo_url>
cd Priority_Engine
python -m venv .venv
source .venv/bin/activate  # Mac/Linux
# or
.\.venv\Scripts\activate  # Windows

pip install -r requirements.txt
```

### Environment variables
Create a `.env` file (or export variables) as needed:

```bash
OPENAI_API_KEY=...
SERPAPI_KEY=...        # optional (required for SERP enrichment)
INPUT_FILE=data/input/german_90d.csv
OUTPUT_DIR=data/output
SERP_LIMIT=0           # set >0 to enable SERP calls
LLM_MAX_ITEMS=1000
```

### Run the pipeline
```bash
python src/main.py
```

If the dataset is large, the pipeline can pause for manual OpenAI Batch processing. Follow the printed instructions and then resume as directed.

## Inputs
- CSV input derived from GSC exports or pre-processed data.
- Required columns are normalized by the engine (e.g., `query`, `landing page`, `avg. position`, or pre-processed fields like `clicks_prev`).

## Outputs
All outputs are written to `OUTPUT_DIR`:
- `engine_output.csv` – base scoring + cannibalization detection
- `serp_output.csv` – SERP-enriched output (when enabled)
- `llm_stage_1_output.csv` – triage output
- `final_output_full_technical.csv` – full technical dataset
- `final_output_full_readable.csv` – human-readable dataset
- `final_output_for_team.csv` – trimmed, sorted team dataset
- `action_output_llm.csv` – LLM classification of actions
- `german_seo_action_plan.html` – visual report

## Documentation
- **System documentation:** [`SYSTEM_DOCUMENTATION.md`](SYSTEM_DOCUMENTATION.md)

## Repo layout
- `src/main.py` – pipeline entry point
- `src/engine_core.py` – scoring and candidate selection
- `src/serp_enrichment.py` – SERP analysis and competition scoring
- `src/llm_stage_1_triage.py` – LLM triage
- `src/llm_stage_2_deepdive.py` – LLM deep dive and exports
- `src/interpretation_layer.py` – LLM action classification
- `src/report_generator.py` – HTML report generator
