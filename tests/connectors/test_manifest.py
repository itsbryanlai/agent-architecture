"""The manifest's low_risk flag: which actions may carry it (D-43)."""

import pytest
import yaml

from agentos.connectors.manifest import load_manifest


@pytest.mark.parametrize(
    ("risk", "idempotency", "flag", "expected"),
    [
        ("write_reversible", "marker_text", True, True),
        ("write_reversible", "marker_text", False, False),
        ("write_reversible", "revision_suggest", True, True),
        ("write_irreversible", "revision", True, "refused"),
        ("write_irreversible", "marker_property", True, "refused"),
        ("write_reversible", "marker_property", True, "refused"),
        ("write_irreversible", "append_text", True, "refused"),
        ("write_irreversible", "replace_body", True, "refused"),
        ("read", None, True, "refused"),
        ("read", "marker_text", True, "refused"),
    ],
    ids=[
        "comment",
        "comment-default",
        "suggest",
        "replace-section",
        "create",
        "create-reversible",
        "append",
        "replace",
        "read",
        "read-marker-text",
    ],
)
def test_the_low_risk_flag_is_accepted_only_on_a_comment_action(
    tmp_path, risk, idempotency, flag, expected
):
    """Fails if low_risk is accepted on a create, append or replace, or refused on a suggestion."""
    spec = {"risk": risk, "description": "An action."}
    if idempotency is not None:
        spec["idempotency"] = idempotency
    if flag:
        spec["low_risk"] = True
    path = tmp_path / "manifest.yaml"
    path.write_text(yaml.safe_dump({"connector": "test", "tools": {"act": spec}}))

    if expected == "refused":
        with pytest.raises(ValueError, match="low_risk"):
            load_manifest(path)
    else:
        assert load_manifest(path).tools["act"].low_risk is expected
