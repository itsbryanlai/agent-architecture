from agentos import ids


def test_ids_format():
    assert ids.conversation_id("acme", "C123", "1700000000.000100") == (
        "acme:conv:C123:1700000000.000100"
    )
    assert ids.task_id("acme", "6f1c") == "acme:task:6f1c"
