from abc import ABC, abstractmethod
from datetime import date, datetime, time, timedelta
from typing import Any, ClassVar, Dict, List, Optional, Set

from neo4j.time import Date as Neo4jDate
from neo4j.time import DateTime as Neo4jDateTime
from neo4j.time import Time as Neo4jTime
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    PrivateAttr,
    field_validator,
    model_validator,
)


class CommonModel(BaseModel, ABC):
    model_config = ConfigDict(
        validate_assignment=True,
        extra="forbid",
        arbitrary_types_allowed=True,
    )

    created: datetime = Field(
        default_factory=datetime.now, json_schema_extra={"set_on_create": True}
    )
    merged: Optional[datetime] = Field(default=None, validate_default=True)

    _set_on_match: List[str] = PrivateAttr()
    _set_on_create: List[str] = PrivateAttr()
    _always_set: List[str] = PrivateAttr()

    _neo4j_supported_types: ClassVar[Any] = (
        list,
        bool,
        int,
        bytearray,
        float,
        str,
        bytes,
        date,
        time,
        datetime,
        timedelta,
    )

    def __init__(self, **data: dict):
        super().__init__(**data)

        self._set_on_match = self._get_prop_usage("set_on_match")
        self._set_on_create = self._get_prop_usage("set_on_create")
        self._always_set = [
            x
            for x in self.model_dump().keys()
            if x not in self._set_on_match + self._set_on_create + ["source", "target"]
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

        prop_values = {
            k: v for k, v in self.neo4j_dict(exclude=exclude).items() if k in props
        }

        return prop_values

    @abstractmethod
    def _get_merge_parameters(self) -> Dict[str, Any]:
        raise NotImplementedError

    @classmethod
    def export_type_converter(cls, value: Any) -> Any:
        if isinstance(value, dict):
            raise TypeError("Neo4j doesn't support dict types for properties.")

        elif isinstance(value, (tuple, set)):
            new_value = list(value)
            return cls.export_type_converter(new_value)

        elif isinstance(value, list):
            # items in a list must all be the same type
            item_type = type(value[0])
            for item in value:
                if isinstance(item, item_type) is False:
                    raise TypeError(
                        "For neo4j, all items in a list must be of the same type."
                    )

            return [cls.export_type_converter(x) for x in value]

        elif isinstance(value, cls._neo4j_supported_types) is False:
            return str(value)

        else:
            return value

    @classmethod
    def _export_dict_converter(cls, original_dict: Dict[str, Any]) -> Dict[str, Any]:
        """_summary_

        Args:
            export_dict (Dict[str, Any]): _description_

        Returns:
            Dict[str, Any]: _description_
        """

        export_dict = original_dict.copy()

        for k, v in export_dict.items():
            export_dict[k] = cls.export_type_converter(v)

        return export_dict

    def neo4j_dict(self, **kwargs: Dict[str, Any]) -> Dict[str, Any]:
        """Return a dict made up of only types compatible with neo4j

        Returns:
            dict: a dictionary export of this model instance
        """

        export_dict = self.model_dump(exclude_none=True, **kwargs)

        export_dict = self._export_dict_converter(export_dict)

        return export_dict

    #
    # validators
    #

    @field_validator("merged")
    def set_merged_to_created(
        cls, value: Optional[datetime], values: Dict[str, Any]
    ) -> datetime:
        """By default, set the 'merged' time equal to the 'created' time.

        If the 'merged' value has been explicitly set, this is preserved.

        Args:
            value (Optional[datetime]): the value of the field.
            values (Dict[str, Any]): a dictionary of field/value pairs set so far.

        Returns:
            datetime: The merged datetime value.
        """

        if value is None:
            return values.data["created"]
        else:
            return value

    @model_validator(mode="before")
    @classmethod
    def neo4j_datetime_to_native(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        """Datetimes come back from Neo4j as a non standard DateTime type.

        We check for any values where that is the case and convert them to
            native Python datetimes.

        See https://neo4j.com/docs/api/python-driver/4.4/temporal_types.html for further info.

        Args:
            values (Dict[str, Any]): Dictionary of field/value pairs from pydantic.

        Returns:
            Dict[str, Any]: Returns the dictionary, with any Neo4jDateTimes updated.
        """

        if not isinstance(values, dict):
            raise ValueError

        for key in values:
            if isinstance(values[key], (Neo4jDateTime, Neo4jDate, Neo4jTime)):
                values[key] = values[key].to_native()

        return values
