from typing import Any

from pydantic import NonNegativeInt, Strict, StringConstraints, TypeAdapter, ValidationError
from typing_extensions import Annotated

GQLIdentifier = Annotated[str, StringConstraints(strict=True, pattern=r"^[a-zA-Z][a-zA-Z0-9_]*$")]

# simple pydantic TypeAdapter for use validating strings before
# inserting them into GQL statements
gql_identifier_adapter = TypeAdapter(GQLIdentifier)

# counts such as SKIP, LIMIT and path depth: strict, so "5" and True are refused
# rather than coerced
non_negative_int_adapter = TypeAdapter(Annotated[NonNegativeInt, Strict()])


def validate_model_identifier(model: type, attribute: str, value: Any) -> None:
    """Check an identifier a model class declares, such as its primary label, as the class is defined.

    Model identifiers are interpolated into queries, so one which is not a valid
    identifier could never be used safely. Raising while the class is defined reports
    it at the class, rather than at whichever query first uses it.

    Args:
        model (type): the class declaring the identifier.
        attribute (str): the attribute it is declared as, such as `__primarylabel__`.
        value (Any): the identifier.

    Raises:
        ValueError: if the value is not a valid identifier.
    """
    try:
        gql_identifier_adapter.validate_python(value)
    except ValidationError as exc:
        raise ValueError(
            f"{model.__name__}.{attribute} is {value!r}, which is not a valid identifier:"
            " it must begin with a letter and contain only letters, digits and underscores."
        ) from exc
