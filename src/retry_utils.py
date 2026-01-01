# src/retry_utils.py
"""
Retry utilities for API calls using tenacity library.

IMPORTANT #7 fix: Replaces custom retry logic with battle-tested tenacity.

Usage:
    from retry_utils import retry_call, LLM_RETRY_CONFIG
    
    response = retry_call(
        client.chat.completions.create,
        kwargs={"model": "gpt-4o-mini", "messages": [...]},
        config=LLM_RETRY_CONFIG
    )
"""

from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
    after_log,
)
import logging
from dataclasses import dataclass
from typing import Callable, Any, Optional
from openai import (
    APIError,
    APITimeoutError,
    RateLimitError,
    APIConnectionError,
)

logger = logging.getLogger(__name__)


@dataclass
class RetryConfig:
    """Retry configuration - supports both naming conventions"""
    max_retries: int = 3
    # Primary names
    min_wait: float = 1.0
    max_wait: float = 60.0
    multiplier: float = 2.0
    # Aliases (for backward compatibility)
    base_delay: float = None
    max_delay: float = None
    exponential_base: float = None
    jitter: bool = False
    
    def __post_init__(self):
        # Handle aliases
        if self.base_delay is not None:
            self.min_wait = self.base_delay
        if self.max_delay is not None:
            self.max_wait = self.max_delay
        if self.exponential_base is not None:
            self.multiplier = self.exponential_base


# Predefined configs
LLM_RETRY_CONFIG = RetryConfig(
    max_retries=4,
    min_wait=1.5,
    max_wait=90.0,
    multiplier=2.0
)

SERP_RETRY_CONFIG = RetryConfig(
    max_retries=3,
    min_wait=2.0,
    max_wait=30.0,
    multiplier=2.0
)

CONSERVATIVE_RETRY_CONFIG = RetryConfig(
    max_retries=2,
    min_wait=1.0,
    max_wait=10.0,
    multiplier=2.0
)


class RetryStats:
    """Track retry statistics"""
    def __init__(self):
        self.total_calls = 0
        self.successful_calls = 0
        self.failed_calls = 0
        self.total_retries = 0
        
    def record_success(self, retries: int = 0):
        self.total_calls += 1
        self.successful_calls += 1
        self.total_retries += retries
        
    def record_failure(self, error_type: str):
        self.total_calls += 1
        self.failed_calls += 1
        
    def get_stats(self):
        return {
            "total_calls": self.total_calls,
            "successful_calls": self.successful_calls,
            "failed_calls": self.failed_calls,
            "success_rate": f"{(self.successful_calls / max(self.total_calls, 1)) * 100:.1f}%",
            "total_retries": self.total_retries,
            "avg_retries_per_success": f"{self.total_retries / max(self.successful_calls, 1):.2f}",
        }


def retry_call(
    func: Callable,
    args: tuple = (),
    kwargs: dict = None,
    config: RetryConfig = None,
    on_retry: Optional[Callable] = None,
) -> Any:
    """
    Call a function with retry logic using tenacity.
    
    Args:
        func: Function to call
        args: Positional arguments
        kwargs: Keyword arguments
        config: RetryConfig instance
        on_retry: Optional callback(attempt, exception, delay)
    
    Returns:
        Function result
        
    Raises:
        Exception after max retries exceeded
        
    Example:
        response = retry_call(
            client.chat.completions.create,
            kwargs={
                "model": "gpt-4o-mini",
                "messages": [{"role": "user", "content": "hi"}]
            },
            config=LLM_RETRY_CONFIG
        )
    """
    if config is None:
        config = LLM_RETRY_CONFIG
    
    if kwargs is None:
        kwargs = {}
    
    # Track attempt number for callback
    attempt_counter = [0]
    
    def custom_before_sleep(retry_state):
        """Called before each retry"""
        attempt_counter[0] += 1
        
        if on_retry:
            exception = retry_state.outcome.exception()
            # Approximate delay (tenacity calculates internally)
            delay = config.min_wait * (config.multiplier ** attempt_counter[0])
            delay = min(delay, config.max_wait)
            on_retry(attempt_counter[0] - 1, exception, delay)
    
    # Create decorated function with retry logic
    @retry(
        stop=stop_after_attempt(config.max_retries + 1),
        wait=wait_exponential(
            multiplier=config.multiplier,
            min=config.min_wait,
            max=config.max_wait
        ),
        retry=retry_if_exception_type((
            APIError,
            APITimeoutError,
            RateLimitError,
            APIConnectionError,
        )),
        before_sleep=custom_before_sleep,
        reraise=True,
    )
    def _wrapped():
        return func(*args, **kwargs)
    
    return _wrapped()


# Convenience decorators
def with_llm_retry(func):
    """
    Decorator for LLM API calls.
    
    Usage:
        @with_llm_retry
        def call_openai(messages):
            return client.chat.completions.create(
                model="gpt-4o-mini",
                messages=messages
            )
    """
    @retry(
        stop=stop_after_attempt(LLM_RETRY_CONFIG.max_retries + 1),
        wait=wait_exponential(
            multiplier=LLM_RETRY_CONFIG.multiplier,
            min=LLM_RETRY_CONFIG.min_wait,
            max=LLM_RETRY_CONFIG.max_wait
        ),
        retry=retry_if_exception_type((
            APIError,
            APITimeoutError,
            RateLimitError,
            APIConnectionError,
        )),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        after=after_log(logger, logging.INFO),
        reraise=True,
    )
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)
    
    return wrapper


def with_serp_retry(func):
    """
    Decorator for SERP API calls.
    
    Usage:
        @with_serp_retry
        def fetch_serp(query):
            return GoogleSearch(params).get_dict()
    """
    @retry(
        stop=stop_after_attempt(SERP_RETRY_CONFIG.max_retries + 1),
        wait=wait_exponential(
            multiplier=SERP_RETRY_CONFIG.multiplier,
            min=SERP_RETRY_CONFIG.min_wait,
            max=SERP_RETRY_CONFIG.max_wait
        ),
        retry=retry_if_exception_type(Exception),  # SERP API uses generic exceptions
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)
    
    return wrapper


# Export
__all__ = [
    'retry_call',
    'RetryConfig',
    'RetryStats',
    'LLM_RETRY_CONFIG',
    'SERP_RETRY_CONFIG',
    'CONSERVATIVE_RETRY_CONFIG',
    'with_llm_retry',
    'with_serp_retry',
]