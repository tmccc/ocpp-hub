"""The local copy of the hassfest translation rules."""

from __future__ import annotations

import json

from tests.tool_loader import load_tool

translations = load_tool("check_translations")


# Intent: angle-bracket placeholders are reported as HTML, plain text and URLs are not.
# Why: hassfest rejected "ws://<Home Assistant IP>:<port>" on the first CI run; the same check must fail locally.
def test_html_like_text_is_reported_with_its_path() -> None:
    data = {"config": {"step": {"user": {"description": "Use ws://<Home Assistant IP>:<port>", "title": "OCPP Hub"}}}}

    problems = translations.html_problems(data)

    assert len(problems) == 1 and problems[0].startswith("config.step.user.description")
    assert translations.html_problems({"a": "Use ws:// then the address, a colon and the port (1 < 2 > 0)."}) == []


# Intent: the shipped translation files pass every local rule.
# Why: this is the same check CI runs, so a failure here is a failure in CI.
def test_shipped_translations_are_clean() -> None:
    for path in translations.translation_files():
        data = json.loads(path.read_text(encoding="utf-8"))
        assert translations.html_problems(data) == [] and translations.issue_problems(data) == [], path


# Intent: the local release check visits the active thin client's own source strings.
# Why: a translation mistake in the deployed client must fail the same check as a mistake in the older integration.
def test_check_reports_bad_proxy_translation(tmp_path, monkeypatch, capsys):
    legacy = tmp_path / "custom_components" / "ocpp_hub"
    legacy.mkdir(parents=True)
    (legacy / "strings.json").write_text("{}")
    component = tmp_path / "custom_components" / "ics_proxy"
    component.mkdir(parents=True)
    (component / "strings.json").write_text(json.dumps({"test": "Use <port>"}))
    monkeypatch.setattr(translations, "__file__", str(tmp_path / "tools" / "check_translations.py"))

    assert translations.main() == 1
    assert "ics_proxy/strings.json" in capsys.readouterr().out
