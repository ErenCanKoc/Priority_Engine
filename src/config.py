import os
from dotenv import load_dotenv

load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
SERPAPI_KEY = os.getenv("SERPAPI_KEY")

# Paths - with sensible defaults
INPUT_FILE = os.getenv("INPUT_FILE", "data/input/german_90d.csv")
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "data/output")

# Limits
SERP_LIMIT = int(os.getenv("SERP_LIMIT", 0))
LLM_MAX_ITEMS = int(os.getenv("LLM_MAX_ITEMS", 1000)) 

PERIOD_MONTHS = 3.0
ACTION_PERCENTILE = 70
