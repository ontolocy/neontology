"""The library must work without its optional extras installed.

Two things are optional: the NetworkX engine ([grand]) and dataframe support
([pandas]). `neontology.graphengines` and `init_neontology` swallow the ImportError
for the former, and `merge_df` defers importing pandas until it is called. These tests
hold that contract, and the CI job that installs without extras exercises the absent
case.
"""

from typing import ClassVar, Optional

import pytest

import neontology
from neontology import graphengines

try:
    from neontology.graphengines import NetworkxConfig  # noqa: F401

    HAS_GRAND = True

except ImportError:
    HAS_GRAND = False

try:
    import pandas  # noqa: F401

    HAS_PANDAS = True

except ImportError:
    HAS_PANDAS = False


def test_neontology_imports_without_the_grand_extra():
    """Importing the package must never require the optional dependency."""
    assert neontology.BaseNode is not None
    assert neontology.init_neontology is not None


def test_networkx_engine_exported_only_when_installed():
    """The optional engine appears in __all__ exactly when its extra is present."""
    assert ("NetworkxEngine" in graphengines.__all__) is HAS_GRAND
    assert ("NetworkxConfig" in graphengines.__all__) is HAS_GRAND


@pytest.mark.skipif(HAS_GRAND, reason="only meaningful without the [grand] extra")
def test_networkx_engine_import_raises_without_extra():
    """Importing the engine module directly should fail cleanly, not partially."""
    with pytest.raises(ImportError):
        from neontology.graphengines.networkxengine import NetworkxEngine  # noqa: F401


def test_unknown_engine_name_explains_itself(monkeypatch):
    """An unrecognised NEONTOLOGY_ENGINE should say what is available."""
    monkeypatch.setenv("NEONTOLOGY_ENGINE", "NOT_AN_ENGINE")

    with pytest.raises(ValueError, match="Available engines"):
        neontology.init_neontology()


@pytest.mark.skipif(HAS_GRAND, reason="only meaningful without the [grand] extra")
def test_requesting_networkx_without_extra_points_at_the_extra(monkeypatch):
    """Asking for NETWORKX without grand installed should name the extra, not KeyError."""
    monkeypatch.setenv("NEONTOLOGY_ENGINE", "NETWORKX")

    with pytest.raises(ValueError, match="grand"):
        neontology.init_neontology()


@pytest.mark.skipif(HAS_PANDAS, reason="only meaningful without the [pandas] extra")
def test_merge_df_explains_how_to_install_pandas():
    """merge_df must name the extra, and point at the alternative that needs nothing."""

    class PandaslessNode(neontology.BaseNode):
        __primaryproperty__: ClassVar[str] = "name"
        __primarylabel__: ClassVar[Optional[str]] = "PandaslessNode"

        name: str

    with pytest.raises(ImportError, match="neontology\\[pandas\\]") as excinfo:
        PandaslessNode.merge_df(None)

    assert "merge_records" in str(excinfo.value)


def test_merge_records_needs_no_optional_dependencies():
    """The canonical ingest path must not reach for pandas."""
    import inspect

    source = inspect.getsource(neontology.BaseNode.merge_records)

    assert "pandas" not in source
    assert "require_pandas" not in source
