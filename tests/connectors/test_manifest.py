"""The manifest's low_risk flag: which actions may carry it (D-43)."""

import pytest
import yaml

from agentos.connectors.manifest import load_manifest


@pytest.mark.parametrize(
    ("risk", "idempotency", "flag", "expected"),
    [
        ("write_reversible", "marker_text", True, True),
        ("write_reversible", "marker_text", False, False),
        ("write_irreversible", "marker_property", True, "refused"),
        ("write_reversible", "marker_property", True, "refused"),
        ("write_irreversible", "append_text", True, "refused"),
        ("write_irreversible", "replace_body", True, "refused"),
        ("read", None, True, "refused"),
    ],
    ids=["comment", "comment-default", "create", "create-reversible", "append", "replace", "read"],
)
def test_the_low_risk_flag_is_accepted_only_on_a_comment_action(
    tmp_path, risk, idempotency, flag, expected
):
    """Fails if loading accepts low_risk on a create, append, replace or read, refuses it on a
    comment, or reads a missing flag as anything but false."""
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
