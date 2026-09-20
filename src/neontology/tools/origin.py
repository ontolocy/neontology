"""Where a record being imported came from.

Every record carries an origin from the moment it is read, so anything reported about
it can say which file and which entry it was - the thing a bare pydantic error, raised
from somewhere inside a batch of five thousand records, cannot tell you.
"""

from dataclasses import dataclass, field, replace
from typing import Any, Optional


@dataclass(frozen=True)
class RecordOrigin:
    """Where one record came from.

    Attributes:
        source: the file it was read from, or a description of the input it came in.
        document: which document within the file, where a file holds several.
        index: which record within that document or list.
        line: which line of the file, where the format is one record per line.
        path: where inside the record, for a record nested in another one - for
            example `RELATIONSHIPS_OUT[0].TARGET_NODES[1]`.
    """

    source: str
    document: Optional[int] = None
    index: Optional[int] = None
    line: Optional[int] = None
    path: Optional[str] = None

    def nested(self, path: str) -> "RecordOrigin":
        """Get the origin of a record nested inside this one.

        Args:
            path (str): where inside this record it is, e.g. `TARGET_NODES[1]`.

        Returns:
            RecordOrigin: the nested record's origin.
        """
        joined = f"{self.path}.{path}" if self.path else path

        return replace(self, path=joined)

    def at(self, index: int) -> "RecordOrigin":
        """Get the origin of the record at a position within this one.

        Args:
            index (int): the position, counting from zero.

        Returns:
            RecordOrigin: that record's origin.
        """
        if self.index is None:
            return replace(self, index=index)

        # already positioned, so this is a position within the record rather than
        # alongside it - which the path describes
        return self.nested(f"[{index}]")

    def at_line(self, line: int) -> "RecordOrigin":
        """Get the origin of the record on one line of the file.

        Args:
            line (int): the line in the file, counting from one as an editor does.

        Returns:
            RecordOrigin: an origin naming that line.
        """
        return replace(self, line=line)

    def in_document(self, document: int) -> "RecordOrigin":
        """Get the origin of a record in one document of a multi-document file.

        Args:
            document (int): which document, counting from zero.

        Returns:
            RecordOrigin: an origin naming that document.
        """
        return replace(self, document=document)

    def __str__(self) -> str:
        """Describe the origin the way it is reported to whoever wrote the content.

        Returns:
            str: the file, and where in it, counting from one as an editor does.
        """
        parts = [self.source]

        if self.document is not None:
            parts.append(f"document {self.document + 1}")

        if self.index is not None:
            parts.append(f"record {self.index + 1}")

        if self.line is not None:
            parts.append(f"line {self.line}")

        described = ", ".join(parts)

        if self.path:
            described = f"{described} ({self.path})"

        return described


@dataclass
class SourcedRecord:
    """One raw record, and where it came from.

    Attributes:
        data: the record as it was written.
        origin: where it was read from.
    """

    data: dict[str, Any] = field(default_factory=dict)
    origin: RecordOrigin = field(default_factory=lambda: RecordOrigin(source="<records>"))
