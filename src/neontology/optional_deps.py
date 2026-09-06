"""Helpers for dependencies which are optional extras rather than requirements.

Neontology's core works with plain Python data structures. Integrations that need a
third party library - pandas dataframes, the NetworkX engine - are optional extras, so
installing neontology stays light for people who do not use them.
"""

from types import ModuleType


def require_pandas() -> ModuleType:
    """Import pandas, explaining how to install it if it is missing.

    Returns:
        ModuleType: the pandas module.

    Raises:
        ImportError: if pandas is not installed, with instructions for installing it
            and a pointer to the alternative that needs no extra dependency.
    """
    try:
        import pandas

    except ImportError as exc:  # pragma: no cover - exercised by the minimal install job
        raise ImportError(
            "This feature needs pandas, which is not installed."
            " Install it with 'pip install neontology[pandas]',"
            " or use merge_records() with a list of dictionaries, which needs no extra dependencies."
        ) from exc

    return pandas
