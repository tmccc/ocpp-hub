"""Validation tools retain real pytest execution failures and never fabricate mutation coverage."""

import os
import py_compile
import subprocess
import sys

import pytest

from tools import mutation_check, validate


# Intent: a later passing command cannot overwrite the independently named failure of an earlier check.
# Why: aggregate validation must preserve every child outcome even when temporary-cache settings are added.
def test_named_steps_preserve_earlier_process_failure():
    results = {}
    validate.run_step("failed check", [sys.executable, "-B", "-c", "raise SystemExit(7)"], results)
    validate.run_step("passing check", [sys.executable, "-B", "-c", "pass"], results)
    assert results == {"failed check": False, "passing check": True}


# Intent: known assertion failures are the only nonzero status the Windows baseline may waive.
# Why: missing collection, usage errors and broken runners have no trustworthy passing evidence.
@pytest.mark.parametrize("code", [1, 2, 3, 4, 5, 7])
def test_pytest_exit_without_failures_never_becomes_success(code):
    result = subprocess.CompletedProcess([], code, "no tests executed", "")
    assert not validate.pytest_succeeded(result, [], [])


# Intent: a known assertion can be waived, while an ERROR or an unrelated failed assertion cannot.
# Why: baseline compatibility must not excuse a test runner or fixture failure.
@pytest.mark.parametrize(
    "output,known,new,ok",
    [
        ("FAILED test_x", ["test_x"], [], True),
        ("ERROR test_x", ["test_x"], [], False),
        ("FAILED test_y", ["test_x"], ["test_y"], False),
    ],
)
def test_pytest_baseline_is_limited_to_known_assertions(output, known, new, ok):
    assert validate.pytest_succeeded(subprocess.CompletedProcess([], 1, output, ""), known, new) is ok


# Intent: actual usage and empty-collection exits through validate.main stay failures despite no FAILED summary.
# Why: this reproduces the false-green bug at the public tool boundary with a real child pytest process.
@pytest.mark.parametrize("usage", [False, True])
def test_real_validator_rejects_unexecuted_pytest(tmp_path, monkeypatch, usage):
    tests = tmp_path / "tests"
    tests.mkdir()
    (tests / "test_subject.py").write_text("def test_one():\n    assert True\n" if usage else "# no tests\n")
    monkeypatch.setattr(validate, "ROOT", tmp_path)
    monkeypatch.setattr(sys, "argv", ["validate", "--tests-only", "-k", "[" if usage else "test"])
    assert validate.main() == 1


# Prepare one genuinely exercised pure module under the task's pytest scratch directory.
def mutation_project(path, body):
    target = path / "subject.py"
    original = "def positive(value):\n    return value > 0\n"
    target.write_text(original)
    (path / "test_subject.py").write_text("from subject import positive\n" + body)
    return target, original


# Intent: broken baseline commands or assertions cannot be reported as mutations caught, and source is restored.
# Why: every score needs passing unmodified tests and completed mutant execution.
@pytest.mark.parametrize("problem", ["usage", "baseline", "mutant_runner"])
def test_mutation_runner_refuses_invalid_evidence(tmp_path, monkeypatch, capsys, problem):
    body = "def test_positive():\n    assert positive(1) and not positive(-1)\n"
    if problem == "baseline":
        body = "def test_baseline():\n    assert False\n"
    elif problem == "mutant_runner":
        body = "import pytest\nif positive(1) is not True:\n    pytest.exit('unusable runner', returncode=4)\n" + body
    target, original = mutation_project(tmp_path, body)
    monkeypatch.setattr(mutation_check, "ROOT", tmp_path)
    args = [str(target), "test_subject.py", *(["--invalid-option"] if problem == "usage" else [])]
    assert mutation_check.main(args) == 2
    output = capsys.readouterr()
    assert "INVALID mutation run" in output.err and " caught" not in output.out
    assert target.read_text() == original


# Intent: a passing baseline and real failing mutant tests produce a score and restore the complete original bytes.
# Why: refusing broken runners must retain useful mutation detection for an exercised source file.
def test_mutation_runner_accepts_real_test_detection(tmp_path, monkeypatch, capsys):
    target, original = mutation_project(tmp_path, "def test_positive():\n    assert positive(1) and not positive(-1)\n")
    monkeypatch.setattr(mutation_check, "ROOT", tmp_path)
    assert mutation_check.main([str(target), "test_subject.py"]) == 0
    assert "0 survived" in capsys.readouterr().out and target.read_text() == original


# Intent: equal-size same-mtime bytecode cannot replace the exact mutant under test.
# Why: an untested second comparison must survive even if the first comparison's cached mutant fails.
def test_mutation_runner_ignores_stale_same_size_bytecode(tmp_path, monkeypatch, capsys):
    original = "def first(v):\n    return v == 1\n\ndef second(v):\n    return v == 1\n"
    target = tmp_path / "subject.py"
    target.write_text(original)
    stale = original.replace("v == 1", "v != 1", 1)
    target.write_text(stale)
    py_compile.compile(str(target), doraise=True)
    stat = target.stat()
    target.write_text(original)
    # AST unparse drops the final newline, so the first equality mutation has this exact cached size.
    assert len(stale) == len(original)
    os.utime(target, ns=(stat.st_atime_ns, stat.st_mtime_ns))
    (tmp_path / "test_subject.py").write_text("from subject import first\ndef test_first():\n    assert first(1)\n")
    monkeypatch.setattr(mutation_check, "ROOT", tmp_path)
    assert mutation_check.main([str(target), "test_subject.py"]) == 1
    assert "SURVIVED compare at line 5" in capsys.readouterr().out
    assert target.read_text() == original
