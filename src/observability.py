# src/observability.py
import time
import logging
from contextlib import contextmanager

# -----------------------
# Logger
# -----------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(stage)s | %(message)s"
)

logger = logging.getLogger("seo_pipeline")

def log(stage, level, message, **kwargs):
    extra = {"stage": stage}
    msg = message
    if kwargs:
        msg += " | " + ", ".join([f"{k}={v}" for k, v in kwargs.items()])
    getattr(logger, level)(msg, extra=extra)

# -----------------------
# Timer
# -----------------------
@contextmanager
def track_stage(stage_name):
    start = time.time()
    yield
    duration = round(time.time() - start, 2)
    log(stage_name, "info", "stage_completed", duration_sec=duration)

# -----------------------
# Metrics collector
# -----------------------
class Metrics:
    def __init__(self, stage):
        self.stage = stage
        self.total = 0
        self.success = 0
        self.failed = 0

    def inc_total(self): self.total += 1
    def inc_success(self): self.success += 1
    def inc_failed(self): self.failed += 1

    def emit(self):
        log(
            self.stage,
            "info",
            "stage_metrics",
            rows_total=self.total,
            rows_success=self.success,
            rows_failed=self.failed
        )
        return {
            "stage": self.stage,
            "rows_total": self.total,
            "rows_success": self.success,
            "rows_failed": self.failed,
        }