from typing import Any

from pydantic import BaseModel, ConfigDict, PrivateAttr

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
        """Set the properties that are used by Neontology for specific purposes.

        This method initializes the class attributes `_set_on_match`, `_set_on_create`, and `_always_set`
        based on the model's JSON schema.
        It retrieves properties that are marked for specific usage types such as 'set_on_match' and 'set_on_create'.
        It also sets the `_always_set` attribute to include properties that are not in `_set_on_match` or `_set_on_create`
        and are not 'source' or 'target'.
        """
        cls._set_on_match = cls._get_prop_usage("set_on_match")
        cls._set_on_create = cls._get_prop_usage("set_on_create")
        cls._always_set = [
            v.alias if v.alias else x
            for x, v in cls.model_fields.items()
            if x not in cls._set_on_match + cls._set_on_create + ["source", "target"]
        ]

    @classmethod
    def _get_prop_usage(cls, usage_type: str) -> list[str]:
        """Get a list of properties that are used by Neontology for a specific purpose.

        These enable complex creation and merging use cases based on model metadata.

        Args:
            usage_type (str): The type of usage to filter properties by.
                              Can be 'set_on_match', 'set_on_create', or 'always_set'.

        Returns:
            list[str]: A list of property names that match the specified usage type.
        """
        all_props = cls.model_json_schema()["properties"]

        selected_props = []

        for prop, entry in all_props.items():
            if entry.get(usage_type) is True:
                selected_props.append(prop)

        return selected_props

    def _get_prop_values(self, props: list[str], exclude: set[str] = set()) -> dict[str, Any]:
        """Get a dictionary of property values for the given properties.

        Args:
        props (list[str]): List of property names to include in the output.
        exclude (set[str], optional): Properties to exclude from the output. Defaults to set().

        Returns:
            dict[str, Any]: a dictionary of key/value pairs.
        """
        # prop_values = {
        #    k: v for k, v in self._engine_dict(exclude=exclude).items() if k in props
        # }

        return self._engine_dict(exclude=exclude, include=set(props))

    def _engine_dict(self, exclude: set[str] = set(), **kwargs: Any) -> dict[str, Any]:
        """Return a dict made up of only types compatible with the GraphEngine.

        Args:
            exclude (set[str], optional): Properties to exclude from the output. Defaults to set().
            **kwargs: Additional keyword arguments to pass to the model_dump method.

        Returns:
            dict: a dictionary export of this model instance
        """
        pydantic_export_dict = self.model_dump(exclude_none=False, exclude=exclude, by_alias=True, **kwargs)

        # return pydantic_export_dict

        try:
            gc = GraphConnection()
            export_dict = gc.engine.export_dict_converter(pydantic_export_dict)

        except RuntimeError:
            export_dict = pydantic_export_dict

        return export_dict

    def _get_merge_parameters_common(self, exclude: set[str] = set()) -> dict[str, Any]:
        """Input an all properties dictionary, and filter based on property types.

        Args:
            exclude (set[str], optional): Properties to exclude from the output. Defaults to set().

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
