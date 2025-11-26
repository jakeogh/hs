"""Standalone test for SignalException_ module-level attribute access bug."""

import signal
import sys
sys.path.insert(0, '.')

import hs


def test_signal_exception_module_getattr():
    """Test that SignalException_X can be accessed as module attributes.

    The module-level __getattr__ only handles ErrorReturnCode_ prefix,
    but not SignalException_ prefix, despite get_exc_from_name() supporting both.
    """
    # ErrorReturnCode_ works (control case)
    exc1 = hs.ErrorReturnCode_1
    assert exc1.exit_code == 1

    # SignalException_ by number should work but doesn't
    try:
        exc_sig9 = hs.SignalException_9
        assert exc_sig9.exit_code == -9
    except AttributeError as e:
        raise AssertionError(
            f"SignalException_9 not accessible via module attribute: {e}"
        )

    # SignalException_ by name should work but doesn't
    try:
        exc_sigterm = hs.SignalException_SIGTERM
        assert exc_sigterm.exit_code == -signal.SIGTERM
    except AttributeError as e:
        raise AssertionError(
            f"SignalException_SIGTERM not accessible via module attribute: {e}"
        )


if __name__ == "__main__":
    test_signal_exception_module_getattr()
    print("PASSED")
