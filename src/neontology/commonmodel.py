from typing import Any, Dict, List, Set

from pydantic import BaseModel, ConfigDict, PrivateAttr, model_validator
from pydantic_core import PydanticCustomError

from .graphconnection import GraphConnection


class CommonModel(BaseModel):
    model_config = ConfigDict(
        validate_assignment=True,
        extra="forbid",
        arbitrary_types_allowed=True,
    )

    _set_on_match: List[str] = PrivateAttr()
    _set_on_create: List[str] = PrivateAttr()
    _always_set: List[str] = PrivateAttr()
    _never_set: List[str] = PrivateAttr()

    def __init__(self, **data: dict):
        super().__init__(**data)
        self._set_prop_usage()

    @classmethod
    def _set_prop_usage(cls) -> None:
        cls._set_on_match = cls._get_prop_usage("set_on_match")
        cls._set_on_create = cls._get_prop_usage("set_on_create")
        cls._never_set = cls._get_prop_usage("never_set")
        cls._always_set = [
            x
            for x in cls.model_fields.keys()
            if x
            not in cls._set_on_match
            + cls._set_on_create
            + cls._never_set
            + ["source", "target"]
        ]

    @classmethod
    def _get_prop_usage(cls, usage_type: str) -> List[str]:
        all_props = cls.model_json_schema()["properties"]

        selected_props = []

        for prop, entry in all_props.items():
            if entry.get(usage_type) is True:
                selected_props.append(prop)

        return selected_props

    def _get_prop_values(
        self, props: List[str], exclude: Set[str] = set()
    ) -> Dict[str, Any]:
        """

        Returns:
            Dict[str, Any]: a dictionary of key/value pairs.
        """

        # prop_values = {
        #    k: v for k, v in self._engine_dict(exclude=exclude).items() if k in props
        # }

        return self._engine_dict(exclude=exclude, include=set(props))

    def _engine_dict(self, exclude: Set[str] = set(), **kwargs: Any) -> Dict[str, Any]:
        """Return a dict made up of only types compatible with the GraphEngine

        Returns:
            dict: a dictionary export of this model instance
        """
        pydantic_export_dict = self.model_dump(
            exclude_none=False, exclude=exclude, **kwargs
        )

        # return pydantic_export_dict

        try:
            gc = GraphConnection()
            export_dict = gc.engine.export_dict_converter(pydantic_export_dict)

        except RuntimeError:
            export_dict = pydantic_export_dict

        return export_dict

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

    def check_sync_result(self, result: BaseModel) -> None:
        """Checks that a returned result matches the current object based on always_set properties.
        Synchronizing the element_id from result to self
        if applicable.

        Raises ValueError if self and result do not match"""
        if not isinstance(result, type(self)):
            raise ValueError(f"Result type is {type(result)}; expected {type(self)}.")
        for k in self._always_set:
            if getattr(self, k) != getattr(result, k):
                raise ValueError(
                    f"Resulting {type(self)} {result.__repr__} does not match the calling object {self.__repr__}."
                )
        elementidproperty = getattr(self, "__elementidproperty__", None)
        if elementidproperty:  # copy element id from result to self
            setattr(self, elementidproperty, getattr(result, elementidproperty))
