"""Temporal workflows, and the activity options they share."""

from datetime import timedelta

from temporalio.common import RetryPolicy

# Activity timeouts: every activity call passes one of these explicitly.
ACTIVITY_TIMEOUT = timedelta(seconds=30)
LLM_TIMEOUT = timedelta(seconds=90)
# Reading, chunking, embedding and storing one page.
INDEX_TIMEOUT = timedelta(minutes=5)

# Errors a retry cannot fix (policy denials, unknown requester, bad input) fail
# at once; everything else is retried with backoff.
RETRY = RetryPolicy(
    maximum_attempts=5,
    initial_interval=timedelta(seconds=1),
    backoff_coefficient=2.0,
    non_retryable_error_types=["GrantDenied", "UnknownRequester", "ValueError"],
)
