from agentos import ids


def test_ids_format():
    assert ids.conversation_id("acme", "C123", "1700000000.000100") == (
        "acme:conv:C123:1700000000.000100"
    )
    assert ids.task_id("acme", "6f1c") == "acme:task:6f1c"


def test_an_import_audit_key_names_the_tenant_and_the_agent():
    assert ids.audit_import_key("acme", "it-helper") == "acme:audit:import:it-helper"
    assert ids.audit_import_key("globex", "it-helper") != ids.audit_import_key("acme", "it-helper")
