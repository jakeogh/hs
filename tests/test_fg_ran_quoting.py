"""Standalone test for ErrorReturnCode.full_cmd missing shlex-quoting in the _fg=True path.

When a foreground command (_fg=True) exits non-zero, hs builds the `ran` /
`full_cmd` string with a bare `" ".join(cmd)` (around line 1897 in __call__),
instead of the `shlex_quote`-per-arg pattern used by the non-fg path
(around line 976). The result is a RAN: line in the exception message that is
ambiguous and not copy-pasteable when any argv entry contains whitespace
or shell metacharacters.
"""

import shlex
import sys

sys.path.insert(0, ".")

import hs


def test_fg_ran_quotes_whitespace_args():
    """RAN string from _fg=True exception must shlex-quote args with spaces."""
    cmd = hs.Command("false")
    cmd.bake("arg with spaces")
    try:
        cmd(_fg=True)
    except hs.ErrorReturnCode as e:
        parsed = shlex.split(e.full_cmd)
        assert "arg with spaces" in parsed, (
            f"full_cmd not shell-quoted; argv lost on round-trip.\n"
            f"  full_cmd: {e.full_cmd!r}\n"
            f"  shlex.split -> {parsed!r}"
        )
    else:
        raise AssertionError("false should have raised ErrorReturnCode")


def test_fg_ran_quotes_shell_metacharacters():
    """RAN string from _fg=True exception must shlex-quote args with shell metacharacters."""
    tricky = "foo; rm -rf /tmp/$USER && echo pwned"
    cmd = hs.Command("false")
    cmd.bake(tricky)
    try:
        cmd(_fg=True)
    except hs.ErrorReturnCode as e:
        parsed = shlex.split(e.full_cmd)
        assert tricky in parsed, (
            f"full_cmd not shell-quoted; metacharacters would re-execute if pasted.\n"
            f"  full_cmd: {e.full_cmd!r}\n"
            f"  shlex.split -> {parsed!r}"
        )
    else:
        raise AssertionError("false should have raised ErrorReturnCode")


def test_fg_ran_roundtrip_matches_argv():
    """shlex.split(e.full_cmd) should reproduce the original argv exactly."""
    args = ["-ignore", "Path myapps/jakeogh", "-ignore", "Path etc/libvirt"]
    cmd = hs.Command("false")
    cmd.bake(*args)
    try:
        cmd(_fg=True)
    except hs.ErrorReturnCode as e:
        parsed = shlex.split(e.full_cmd)
        # parsed[0] is the resolved abspath of `false`; compare the rest.
        assert parsed[1:] == args, (
            f"Round-trip mismatch:\n"
            f"  full_cmd: {e.full_cmd!r}\n"
            f"  expected tail: {args!r}\n"
            f"  got tail:      {parsed[1:]!r}"
        )
    else:
        raise AssertionError("false should have raised ErrorReturnCode")


if __name__ == "__main__":
    test_fg_ran_quotes_whitespace_args()
    test_fg_ran_quotes_shell_metacharacters()
    test_fg_ran_roundtrip_matches_argv()
    print("PASSED")
