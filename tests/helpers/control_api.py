"""Shared test helpers."""

BASE = "http://127.0.0.1:8100"


JSON = {"Content-Type": "application/json"}


class StandIn:
    """A test identity: whoever the test says is acting. Wave 5 replaces the real one."""

    def __init__(self, user: str):
        self.user = user

    async def actor(self, request) -> str:
        return self.user
