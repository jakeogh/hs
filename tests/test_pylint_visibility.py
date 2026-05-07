"""Test that pylint can see ErrorReturnCode_X and SignalException_X as real module attributes."""

import subprocess
import tempfile
from pathlib import Path

import pytest


def test_pylint_sees_error_return_code():
    """Verify pylint doesn't complain about ErrorReturnCode_X imports."""
    code = """\
from hs import ErrorReturnCode_1, ErrorReturnCode_127, ErrorReturnCode_255
print(ErrorReturnCode_1, ErrorReturnCode_127, ErrorReturnCode_255)
"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
        f.write(code)
        f.flush()
        result = subprocess.run(
            ["pylint", "--errors-only", "--disable=all", "--enable=E0611", f.name],
            capture_output=True,
            text=True,
        )
    Path(f.name).unlink()
    assert "E0611" not in result.stdout, f"pylint found E0611 error:\n{result.stdout}"


def test_pylint_sees_signal_exception_named():
    """Verify pylint doesn't complain about SignalException_SIGXXX imports."""
    code = """\
from hs import SignalException_SIGTERM, SignalException_SIGKILL, SignalException_SIGINT
print(SignalException_SIGTERM, SignalException_SIGKILL, SignalException_SIGINT)
"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
        f.write(code)
        f.flush()
        result = subprocess.run(
            ["pylint", "--errors-only", "--disable=all", "--enable=E0611", f.name],
            capture_output=True,
            text=True,
        )
    Path(f.name).unlink()
    assert "E0611" not in result.stdout, f"pylint found E0611 error:\n{result.stdout}"


def test_pylint_sees_signal_exception_numbered():
    """Verify pylint doesn't complain about SignalException_N imports."""
    code = """\
from hs import SignalException_9, SignalException_15, SignalException_6
print(SignalException_9, SignalException_15, SignalException_6)
"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
        f.write(code)
        f.flush()
        result = subprocess.run(
            ["pylint", "--errors-only", "--disable=all", "--enable=E0611", f.name],
            capture_output=True,
            text=True,
        )
    Path(f.name).unlink()
    assert "E0611" not in result.stdout, f"pylint found E0611 error:\n{result.stdout}"
