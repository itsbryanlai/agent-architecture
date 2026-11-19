"""Temporal workflows, and the activity options they share."""

from datetime import timedelta

from temporalio.common import RetryPolicy

# Activity timeouts: every activity call passes one of these explicitly.
ACTIVITY_TIMEOUT = timedelta(seconds=30)
LLM_TIMEOUT = timedelta(seconds=90)
# A progress edit on the placeholder: short, and at most one retry. The workflow
# awaits it inside the step and drops a failure, so it never delays the reply much
# and never raises into the turn.
PROGRESS_TIMEOUT = timedelta(seconds=5)
PROGRESS_RETRY = RetryPolicy(maximum_attempts=2, initial_interval=timedelta(seconds=1))
# An approved write to Notion: a few requests, each limited to REQUEST_TIMEOUT_MS. The
# activity heartbeats between them, so an attempt that stops making progress is cancelled
# after WRITE_HEARTBEAT_TIMEOUT instead of running on beside its retry. The lease in
# ``external_writes`` outlasts the activity timeout, so a live attempt always holds it.
WRITE_TIMEOUT = timedelta(seconds=60)
WRITE_HEARTBEAT_TIMEOUT = timedelta(seconds=25)
WRITE_LEASE_SECONDS = WRITE_TIMEOUT.total_seconds() + 30
# After retries run out the task looks for the item this many times, this far apart.
RECONCILE_TRIES = 4
RECONCILE_INTERVAL = timedelta(seconds=3)

# A watching task answers a poke from a webhook at most once per this long, so a burst of
# pokes adds a poll, not a history's worth of events.
POKE_GAP = timedelta(seconds=60)
# A watch starts a new run when its history reaches this many events, if Temporal has not
# suggested it sooner. Far below the server's limit, and about 150 polls.
WATCH_HISTORY_BOUND = 1000

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
