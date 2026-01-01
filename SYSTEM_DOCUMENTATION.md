# Priority Engine System Documentation

## 1. System overview
Priority Engine is a batch-oriented SEO analysis pipeline. It ingests GSC-style performance data, applies scoring and candidate selection, optionally enriches rows with SERP features, and uses two LLM stages to produce action recommendations. Outputs include CSV deliverables and an HTML report for stakeholder sharing.

Core goals:
- Identify rescue/scale/expand opportunities with consistent heuristics.
- Adjust expected CTR based on SERP features (when available).
- Produce human-readable, actionable outputs using LLM reasoning.

## 2. Architecture
The system is organized as a linear pipeline orchestrated by `src/main.py`.

### 2.1 Pipeline stages
1. **Engine Core** (`src/engine_core.py`)
   - Normalizes input columns, parses numeric values, and computes CTR expectations.
   - Scores pages for rescue/scale/expand opportunities.
   - Flags cannibalization (multiple URLs for one keyword).

2. **SERP Enrichment (Optional)** (`src/serp_enrichment.py`)
   - Uses SerpAPI to fetch German SERP results.
   - Calculates weighted competition score based on SERP features.

3. **Traffic Gap Recalculation** (`src/main.py`)
   - Recomputes expected clicks and gaps using SERP-aware CTR logic.

4. **LLM Stage 1 – Triage** (`src/llm_stage_1_triage.py`)
   - Reviews candidate rows and returns an initial verdict + reasoning.

5. **LLM Stage 2 – Deep Dive** (`src/llm_stage_2_deepdive.py`)
   - Runs on rows marked “action” by Stage 1.
   - Generates final recommendations and exports readable outputs.

6. **Interpretation Layer** (`src/interpretation_layer.py`)
   - Classifies actions into action types, effort, owner, and priority.

7. **Report Generation** (`src/report_generator.py`)
   - Builds an HTML report for localization/content teams.

### 2.2 High-level data flow
```
input CSV
  → engine_output.csv
  → serp_output.csv (optional, when SERP_LIMIT > 0)
  → llm_stage_1_output.csv
  → final_output_* CSVs
  → action_output_llm.csv
  → german_seo_action_plan.html
```

## 3. Configuration
All configuration values come from environment variables loaded via `.env`.

| Variable | Purpose | Default |
| --- | --- | --- |
| `OPENAI_API_KEY` | OpenAI API key | none |
| `SERPAPI_KEY` | SerpAPI key | none |
| `INPUT_FILE` | Input CSV path | `data/input/german_90d.csv` |
| `OUTPUT_DIR` | Output directory | `data/output` |
| `SERP_LIMIT` | Max SERP lookups (0 disables) | `0` |
| `LLM_MAX_ITEMS` | Max rows for LLM processing | `1000` |

Additional constants:
- `PERIOD_MONTHS` (engine) controls MSV estimation.
- `ACTION_PERCENTILE` controls candidate thresholding.

## 4. Core logic details

### 4.1 Engine scoring
`src/engine_core.py` computes core metrics:
- **Expected CTR** based on rank position.
- **Expected clicks** and **traffic gap**.
- **Rescue/scale/expand scores** and candidate selection logic.

Candidates are filtered for data quality and categorized into:
- `rescue`, `scale`, `expand`, `monitor`, `ignore`

### 4.2 Cannibalization detection
`detect_cannibalization()` flags keywords with multiple ranking URLs. Any cannibalized row is always marked as an analysis candidate and labeled with a group ID.

### 4.3 SERP enrichment
`src/serp_enrichment.py` uses SerpAPI to identify SERP features and calculate a **weighted competition score**. Dominant features are those with weights ≥ 20 (e.g., featured snippets, video, local pack).

SERP features are also used in `src/main.py` to adjust expected CTR (penalties for SERP feature presence).

### 4.4 LLM stages
- **Stage 1 (triage):** outputs `llm_stage_1_verdict`, confidence, and reasoning.
- **Stage 2 (deep dive):** outputs final action recommendations and generates readable exports.

The models default to `gpt-4o-mini` with low temperature. Both stages include retry logic (see `src/retry_utils.py`).

### 4.5 Interpretation layer
After final outputs, the interpretation layer reclassifies actions into:
- `action_type` (e.g., `content_expansion`, `ctr_optimization`)
- `effort`, `owner`, and `priority`

This produces `action_output_llm.csv` for operational execution.

### 4.6 Report generation
`src/report_generator.py` compiles the final outputs into a visual HTML report. It highlights priority pages and includes summary stats for stakeholders.

## 5. Input data contract
The engine accepts either raw GSC-style exports or pre-processed datasets.

**Common columns (normalized to lowercase):**
- `query`, `landing page`, `avg. position`, `impressions`, `url clicks`

**Optional pre-processed columns:**
- `clicks_prev`, `impr_prev` (if provided, percent deltas are not required)

The engine normalizes columns internally and computes missing fields as needed.

## 6. Output files
| File | Description |
| --- | --- |
| `engine_output.csv` | Engine scoring output + cannibalization |
| `serp_output.csv` | SERP-enriched output |
| `llm_stage_1_output.csv` | LLM triage output |
| `final_output_full_technical.csv` | All columns (technical) |
| `final_output_full_readable.csv` | All columns (readable) |
| `final_output_for_team.csv` | Trimmed output for content team |
| `action_output_llm.csv` | Action classification output |
| `german_seo_action_plan.html` | HTML report |

## 7. Operational considerations
- **Cost control:** cap processing with `LLM_MAX_ITEMS`.
- **SERP calls:** set `SERP_LIMIT` to control SerpAPI usage (0 disables).
- **Batch mode:** large datasets trigger a manual OpenAI Batch flow (see console output).

## 8. Observability and logging
- LLM stages log to `data/output/llm_stage_1.log` and `llm_stage_2.log`.
- Metrics are written to JSON in the output directory.
- `src/observability.py` provides helper logging and stage tracking for SERP enrichment.

## 9. Entry points
- **Main pipeline:** `python src/main.py`
- **Individual stages:** can be run via their module scripts (e.g., `python src/serp_enrichment.py`).

## 10. Dependencies
Key dependencies are managed in `requirements.txt`:
- `pandas`, `numpy`, `tqdm`
- `openai`
- `serpapi`
- `python-dotenv`

