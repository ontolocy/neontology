"""What an import did, returned so it can be checked rather than assumed."""

from pydantic import BaseModel, Field


class ImportReport(BaseModel):
    """A summary of one import run.

    Attributes:
        nodes: how many node records were merged, by primary label.
        relationships: how many relationship records were merged, by relationship type.
        files: the files read, where the content came from files.
        unresolved: endpoints which did not identify exactly one node, described. These
            are warnings rather than errors unless `error_on_unmatched` is set, so they
            are reported here to be acted on.
        validated_only: whether the content was checked without being written.
    """

    nodes: dict[str, int] = Field(default_factory=dict)
    relationships: dict[str, int] = Field(default_factory=dict)
    files: list[str] = Field(default_factory=list)
    unresolved: list[str] = Field(default_factory=list)
    validated_only: bool = False

    @property
    def node_count(self) -> int:
        """How many nodes were merged in total.

        Returns:
            int: the total.
        """
        return sum(self.nodes.values())

    @property
    def relationship_count(self) -> int:
        """How many relationships were merged in total.

        Returns:
            int: the total.
        """
        return sum(self.relationships.values())

    def __str__(self) -> str:
        """Describe what the import did.

        Returns:
            str: a one line summary.
        """
        did = "Validated" if self.validated_only else "Imported"

        summary = (
            f"{did} {self.node_count} nodes ({len(self.nodes)} labels)"
            f" and {self.relationship_count} relationships ({len(self.relationships)} types)"
        )

        if self.files:
            summary = f"{summary} from {len(self.files)} files"

        if self.unresolved:
            summary = f"{summary}, with {len(self.unresolved)} unresolved endpoints"

        return summary
