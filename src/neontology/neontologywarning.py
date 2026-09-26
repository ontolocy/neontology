class NeontologyWarning(UserWarning):
    """The base class of the warnings Neontology raises about your models, queries and results.

    Filter on it to handle Neontology's warnings as a class, for example to turn them into
    errors in a test suite: `warnings.simplefilter("error", NeontologyWarning)`. Each is
    still a `UserWarning`. Deprecations are raised as `DeprecationWarning` instead, as
    Python's tooling expects.
    """
