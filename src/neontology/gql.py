from pydantic import StringConstraints, TypeAdapter
from typing_extensions import Annotated

GQLIdentifier = Annotated[
    str, StringConstraints(strict=True, pattern=r"^[a-zA-Z][a-zA-Z0-9_]+$")
]

# simple pydantic TypeAdapter for use validating strings before
# inserting them into GQL statements
gql_identifier_adapter = TypeAdapter(GQLIdentifier)

int_adapter = TypeAdapter(int)
