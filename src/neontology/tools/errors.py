"""Errors raised when importing content into the graph.

Every error here subclasses `ValueError`, which is what the importer has always
raised for malformed content, so existing `except ValueError` callers keep working.
"""

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass

from .origin import RecordOrigin


class ImportContentError(ValueError):
    """Content being imported is malformed, or does not describe the models defined.

    Raised for a file which cannot be parsed, a record which is neither a node nor a
    relationship, a label or relationship type with no model class, a record which does
    not validate against its model, and a relationship endpoint which does not resolve.
    """


class DuplicateNodeDefinitionError(ImportContentError):
    """The same node is defined more than once.

    A node is *defined* by a record at the top level of the content - the root of a
    YAML document, an element of a top level list, an entry under `nodes`, or a
    markdown file's frontmatter. One node has one definition, so a second one is an
    error whether or not the two agree.

    Records nested in `TARGET_NODES` are not definitions: they exist so a relationship
    can bring a node into the graph without a record of its own, and any number of them
    may name the same node.
    """


class ConflictingNodeRecordError(ImportContentError):
    """Two records for the same node give a property two different values.

    Records for one node are combined, so that a node named inline by a relationship
    does not overwrite the properties its definition carries. Combining them is only
    well defined while they agree: where they disagree, neither value can be preferred
    without making the result depend on the order the content happened to be read in.
    """


@dataclass(frozen=True)
class ImportIssue:
    """One problem found in the content, and where it was found.

    Attributes:
        origin: the record the problem is in.
        error: what was wrong with it.
    """

    origin: RecordOrigin
    error: Exception

    def __str__(self) -> str:
        """Describe the problem and where it is.

        Returns:
            str: the origin, followed by the error.
        """
        return f"{self.origin}: {self.error}"


class ImportValidationError(ImportContentError):
    """Content did not validate, listing every problem found rather than only the first.

    Raised by an import run with `validate_only=True`, which checks all of the content
    before reporting, so a repository can be fixed in one pass rather than one error at
    a time.

    Attributes:
        issues: every problem found, in the order the content was read.
    """

    def __init__(self, issues: list[ImportIssue]):
        self.issues = issues

        super().__init__(self._describe(issues))

    @staticmethod
    def _describe(issues: list[ImportIssue]) -> str:
        """Render the issues as a report, grouped by the file they were found in.

        Args:
            issues (list[ImportIssue]): the problems found.

        Returns:
            str: the report.
        """
        count = len(issues)
        heading = f"{count} problem{'s' if count != 1 else ''} in the content to import:"

        lines = [heading]

        # gathered by file rather than grouped as they arrive: the checks run in phases,
        # so issues from one file are not next to each other
        by_source: dict[str, list[ImportIssue]] = {}

        for issue in issues:
            by_source.setdefault(issue.origin.source, []).append(issue)

        for source, grouped in by_source.items():
            lines.append(f"\n{source}")

            for issue in grouped:
                where = str(issue.origin)

                # the source is already the heading for this group
                where = where[len(source) :].lstrip(", ") or "the file itself"

                # a record identified only by where it sits inside another needs no
                # parentheses, having nothing to sit in parentheses after
                if where.startswith("(") and where.endswith(")"):
                    where = where[1:-1]

                # indent the error under where it was found, however many lines it runs to
                detail = str(issue.error).strip().replace("\n", "\n      ")

                lines.append(f"  {where}\n      {detail}")

        return "\n".join(lines)


class ErrorCollector:
    """Gathers problems found in content, or raises them as they are found.

    Validating gathers every problem so the whole of a repository can be fixed in one
    pass. Importing raises the first, because the graph is being written to and there is
    nothing to be gained by carrying on.
    """

    def __init__(self, collect: bool = False):
        self.collect = collect
        self.issues: list[ImportIssue] = []

    def fail(self, origin: RecordOrigin, error: Exception) -> None:
        """Report a problem with a record.

        Args:
            origin (RecordOrigin): the record it is in.
            error (Exception): what is wrong with it.

        Raises:
            Exception: the error, named with its origin, unless collecting.
        """
        if self.collect is True:
            self.issues.append(ImportIssue(origin=origin, error=error))
            return

        raise self._located(origin, error) from error

    @contextmanager
    def catching(self, origin: RecordOrigin) -> Iterator[None]:
        """Report whatever a block raises as a problem with one record.

        This is what puts a file and an entry on a model's own validation error, which
        otherwise says which field is wrong but not which of the records being imported
        it belongs to.

        Args:
            origin (RecordOrigin): the record being handled.

        Yields:
            None: while the block runs.
        """
        try:
            yield

        except Exception as exc:
            self.fail(origin, exc)

    @staticmethod
    def _located(origin: RecordOrigin, error: Exception) -> ImportContentError:
        """Restate an error so it names the record it came from.

        An error of our own keeps its class, so the distinction between a duplicate
        definition and a contradiction survives being located and can still be caught
        separately. Anything else - a model's own validation error, most often - is
        wrapped, since only its message can be carried across.

        Args:
            origin (RecordOrigin): where the record came from.
            error (Exception): the original error.

        Returns:
            ImportContentError: the error, named with its origin.
        """
        # the original is kept as the cause, so the full traceback is still there
        message = f"{origin}:\n{error}"

        if isinstance(error, ImportContentError) and not isinstance(error, ImportValidationError):
            return type(error)(message)

        return ImportContentError(message)

    def raise_if_any(self) -> None:
        """Raise everything collected, if anything was.

        Raises:
            ImportValidationError: listing every problem found.
        """
        if self.issues:
            raise ImportValidationError(self.issues)
