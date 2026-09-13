"""Neontology runs without the GIL on free-threaded builds of Python.

CI runs the suite on free-threaded builds. That only tests what it exists to test while the
GIL really is disabled.
"""

import importlib
import importlib.util
import sys
import sysconfig

import pytest

pytestmark = pytest.mark.skipif(
    not sysconfig.get_config_var("Py_GIL_DISABLED"),
    reason="only meaningful on a free-threaded build of Python",
)


def test_the_gil_stays_disabled():
    """Importing a C extension not declared safe without the GIL turns the GIL back on.

    Python only warns when that happens, so the suite would carry on - with the GIL
    enabled, and no longer testing a free-threaded build. Neontology and whichever of its
    optional extras are installed are imported first, so anything they bring in counts.
    """
    import neontology  # noqa: F401

    for module in ("pandas", "networkx", "grandcypher"):
        if importlib.util.find_spec(module):
            importlib.import_module(module)

    assert sys._is_gil_enabled() is False
