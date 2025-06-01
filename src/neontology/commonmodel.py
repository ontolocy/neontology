from typing import Any

from pydantic import BaseModel, ConfigDict, PrivateAttr, model_validator
from pydantic_core import PydanticCustomError

from .graphconnection import GraphConnection


class CommonModel(BaseModel):
    model_config = ConfigDict(
        validate_assignment=True,
        extra="forbid",
        arbitrary_types_allowed=True,
    )

    _set_on_match: list[str] = PrivateAttr()
    _set_on_create: list[str] = PrivateAttr()
    _always_set: list[str] = PrivateAttr()

    def __init__(self, **data: dict):
        super().__init__(**data)
        self._set_prop_usage()

    @classmethod
    def _set_prop_usage(cls) -> None:
        cls._set_on_match = cls._get_prop_usage("set_on_match")
        cls._set_on_create = cls._get_prop_usage("set_on_create")
        cls._always_set = [
            v.alias if v.alias else x
            for x, v in cls.model_fields.items()
            if x not in cls._set_on_match + cls._set_on_create + ["source", "target"]
        ]

    @classmethod
    def _get_prop_usage(cls, usage_type: str) -> list[str]:
        all_props = cls.model_json_schema()["properties"]

        selected_props = []

        for prop, entry in all_props.items():
            if entry.get(usage_type) is True:
                selected_props.append(prop)

        return selected_props

    def _get_prop_values(
        self, props: list[str], exclude: set[str] = set()
    ) -> dict[str, Any]:
        """

        Returns:
            dict[str, Any]: a dictionary of key/value pairs.
        """

        # prop_values = {
        #    k: v for k, v in self._engine_dict(exclude=exclude).items() if k in props
        # }

        return self._engine_dict(exclude=exclude, include=set(props))

    def _engine_dict(self, exclude: set[str] = set(), **kwargs: Any) -> dict[str, Any]:
        """Return a dict made up of only types compatible with the GraphEngine

        Returns:
            dict: a dictionary export of this model instance
        """

        pydantic_export_dict = self.model_dump(
            exclude_none=False, exclude=exclude, by_alias=True, **kwargs
        )

        # return pydantic_export_dict

        try:
            gc = GraphConnection()
            export_dict = gc.engine.export_dict_converter(pydantic_export_dict)

        except RuntimeError:
            export_dict = pydantic_export_dict

        return export_dict

    def _get_merge_parameters_common(self, exclude: set[str] = set()) -> dict[str,Any]:
        """Input an all properties dictionary, and filter based on property types.

            Returns:
                Dict[str, Any]: Dictionary of always_set, set_on_match, and set_on_create dictionaries
            """
        # get all the properties
        all_props = self._engine_dict(exclude=exclude)

        always_set = {k: all_props[k] for k in self._always_set}
        set_on_match = {k: all_props[k] for k in self._set_on_match}
        set_on_create = {k: all_props[k] for k in self._set_on_create}
        params = {
            "all_props": all_props,
            "always_set": always_set,
            "set_on_match": set_on_match,
            "set_on_create": set_on_create,
        }
        return params

    #
    # validators
    #

    @model_validator(mode="before")
    @classmethod
    def deprecated_merged_created(cls, data: Any) -> Any:
        """Neontology v0 and v1 included and auto-populated this property.
        Flag a warning whe
            temporarily supported/deprecated before being removed.
        """

        if ("created" in data and "created" not in cls.model_fields) or (
            "merged" in data and "merged" not in cls.model_fields
        ):
            raise PydanticCustomError(
                "created_or_merged_fields",
                (
                    "Native neontology support for 'merged' and 'created' properties has been removed."
                    " Consider adding these fields to your model(s) and read the docs for further info"
                ),
            )
        return data
