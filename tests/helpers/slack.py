"""A Slack API refusal as the SDK raises it, for the fakes' injection points."""

from slack_sdk.errors import SlackApiError
from slack_sdk.web.slack_response import SlackResponse


def slack_error(
    code: str, api_url: str = "https://slack.com/api/chat.postMessage"
) -> SlackApiError:
    return SlackApiError(
        "The request to the Slack API failed.",
        SlackResponse(
            client=None,
            http_verb="POST",
            api_url=api_url,
            req_args={},
            data={"ok": False, "error": code},
            headers={},
            status_code=200,
        ),
    )
