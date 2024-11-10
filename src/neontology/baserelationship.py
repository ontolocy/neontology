import itertools
import json
import warnings
from typing import Any, ClassVar, Dict, List, Optional, Type, TypeVar

import numpy as np
import pandas as pd
from pydantic import BaseModel, PrivateAttr, ValidationError, model_validator

from neontology.graphconnection import GraphConnection

from .basenode import BaseNode
from .commonmodel import CommonModel
from .gql import gql_identifier_adapter
from .schema_utils import RelationshipSchema, SchemaProperty, extract_type_mapping

R = TypeVar("R", bound="BaseRelationship")


class BaseRelationship(CommonModel):  # pyre-ignore[13]
    source: BaseNode
    target: BaseNode

    __relationshiptype__: ClassVar[Optional[str]] = None

    _merge_on: List[str] = (
        PrivateAttr()
    )  # what relationship properties should we merge on

    def __init__(self, **data: dict):
        super().__init__(**data)

        # we can define 'abstract' relationships which don't have a label
        # these are to provide common properties to be used by subclassed relationships
        # but shouldn't be put in the graph or even instantiated
        if self.__relationshiptype__ is None:
            raise NotImplementedError(
                "Relationships to be used in the graph must define a relationship type."
            )

    @classmethod
    def _set_prop_usage(cls) -> None:
        super()._set_prop_usage()
        cls._merge_on = cls._get_prop_usage("merge_on")

    @model_validator(mode="after")
    def validate_identifiers(self) -> "BaseRelationship":
        try:
            gql_identifier_adapter.validate_strings(self.__relationshiptype__)
        except AttributeError:
            pass
        except ValidationError:
            warnings.warn(
                (
                    "Relationship Type should contain only alphanumeric characters and underscores."
                    " It should begin with an alphabetic character."
                )
            )

        return self

    @classmethod
    def get_relationship_type(cls) -> Optional[str]:
        """Get the relationship type to use for creating and matching this relationship.

        If __relationship__ has been specified, use that.

        Otherwise use the class name in uppercase

        Returns:
            str: the string to use for creating and matching this relationship
        """
        return cls.__relationshiptype__  # pyre-ignore[7]

    def _get_merge_parameters(
        self, source_prop: str, target_prop: str
    ) -> Dict[str, Any]:
        """

        Returns:
            Dict[str, Any]: a dictionary of key/value pairs.
        """

        exclusions = {"source", "target"}

        # get all the properties
        all_props = self.model_dump(exclude=exclusions)

        # merge_props properties will be referenced individually with kwargs
        merge_props = {k: all_props[k] for k in self._merge_on}

        always_set = {k: all_props[k] for k in self._always_set}

        set_on_match = {k: all_props[k] for k in self._set_on_match}

        set_on_create = {k: all_props[k] for k in self._set_on_create}

        source_prop = self.source.model_dump()[source_prop]
        target_prop = self.target.model_dump()[target_prop]

        params = {
            "source_prop": source_prop,
            "target_prop": target_prop,
            "always_set": always_set,
            "set_on_match": set_on_match,
            "set_on_create": set_on_create,
            **merge_props,
        }

        return params

    def merge(
        self,
    ) -> None:
        """Merge this relationship into the database."""
        source_label = self.source.__primarylabel__
        target_label = self.target.__primarylabel__

        if not source_label or not target_label:
            raise ValueError(
                "Source and target Nodes must have a defined primary label for creating a relationship."
            )

        source_prop = self.source.__primaryproperty__
        target_prop = self.target.__primaryproperty__

        rel_props = self._get_merge_parameters(
            source_prop=source_prop, target_prop=target_prop
        )

        merge_on_props = self._merge_on

        rel_type = self.get_relationship_type()

        if not rel_type:
            raise ValueError(
                "Realtionship must have a defined relationship type for creating a relationship."
            )

        gc = GraphConnection()

        gc.merge_relationships(
            source_label,
            target_label,
            source_prop,
            target_prop,
            rel_type,
            merge_on_props,
            [rel_props],
        )

    @classmethod
    def merge_relationships(
        cls: Type[R],
        rels: List[R],
        source_prop: Optional[str] = None,
        target_prop: Optional[str] = None,
    ) -> None:
        """Merge multiple relationships (of this type) into the database.

        Sometimes the source and target label may be ambiguous (e.g. where we have subclassed nodes)
            In this case you can explicitly pass in the relevant types

        Sometimes we want to match nodes on a property which isn't the primary property,
        so we can specify what property to use.

        Args:
            cls (Type[R]): this class
            rels (List[R]): a list of relationships which are instances of this class

        Raises:
            TypeError: If relationships are provided which aren't of this class
        """

        # define the properties to merge on
        merge_on_props = cls._get_prop_usage("merge_on")

        # sources and targets could have different primary labels
        # to operate efficiently, we group like source and targets for batch creation of relationships
        grouped_rels = itertools.groupby(
            rels, lambda x: (x.source.__class__, x.target.__class__)
        )

        for node_clases, common_rels in grouped_rels:
            src_class = node_clases[0]
            tgt_class = node_clases[1]

            source_label = src_class.__primarylabel__
            target_label = tgt_class.__primarylabel__

            if source_prop is None:
                source_prop = src_class.__primaryproperty__

            if target_prop is None:
                target_prop = tgt_class.__primaryproperty__

            if not source_label or not target_label:
                raise ValueError(
                    "Source and target Nodes must have a defined primary label to create a relationship."
                )

            rel_type = cls.get_relationship_type()

            if not rel_type:
                raise ValueError(
                    "Relationship must have a defined relationship type for creating a relationship."
                )

            rel_list: List[Dict[str, Any]] = [
                x._get_merge_parameters(source_prop, target_prop) for x in common_rels
            ]

            gc = GraphConnection()

            gc.merge_relationships(
                source_label,
                target_label,
                source_prop,
                target_prop,
                rel_type,
                merge_on_props,
                rel_list,
            )

    @classmethod
    def merge_records(
        cls: Type[R],
        records: List[Dict[str, Any]],
        source_type: Optional[Type[BaseNode]] = None,
        target_type: Optional[Type[BaseNode]] = None,
        source_prop: Optional[str] = None,
        target_prop: Optional[str] = None,
    ) -> None:
        """Take a list of dictionaries and use them to merge in relationships in the graph.

        Sometimes, a relationship can accept nodes which subclass a particular node type.
            In these instances, it may be necessary to explicitly state what type of node should be used.

        Each record should have a source and target key where the value is the primary property
            value of the respective nodes.

        Args:
            records (List[Dict[str, Any]]): a list of dictionaries used to populate relationships
            source_type: explicitly state the class to use for source node
            target_type: explicitly state the class to use for target node
        """

        hydrated_list = []

        if source_type is None:
            source_type = cls.model_fields["source"].annotation

        if target_type is None:
            target_type = cls.model_fields["target"].annotation

        if not source_type or not target_type:
            raise ValueError("Source and target Nodes types not defined.")

        if source_prop is None:
            source_prop = source_type.__primaryproperty__

        if target_prop is None:
            target_prop = target_type.__primaryproperty__

        for record in records:
            hydrated = dict(record)

            hydrated["source"] = source_type.model_construct(
                **{source_prop: record["source"]}
            )
            hydrated["target"] = target_type.model_construct(
                **{target_prop: record["target"]}
            )

            hydrated_list.append(cls(**hydrated))

        cls.merge_relationships(
            hydrated_list,
            source_prop=source_prop,
            target_prop=target_prop,
        )

    @classmethod
    def merge_df(
        cls: Type[R],
        df: pd.DataFrame,
        source_type: Optional[Type[BaseNode]] = None,
        target_type: Optional[Type[BaseNode]] = None,
        source_prop: Optional[str] = None,
        target_prop: Optional[str] = None,
    ) -> None:
        """Merge in relationships based on data in a pandas data frame

        Expects columns named 'source' and 'target' with the primary property value
            for the source and target nodes.

        Then additional fields should have a corresponding column.

        Args:
            df (pd.DataFrame): pandas dataframe where each row represents a relationship to merge
        """

        if df.empty is False:
            records = df.replace([np.nan], None).to_dict(orient="records")
            cls.merge_records(
                records,
                source_type=source_type,
                source_prop=source_prop,
                target_type=target_type,
                target_prop=target_prop,
            )

    @classmethod
    def match_relationships(
        cls, limit: Optional[int] = None, skip: Optional[int] = None
    ) -> List["BaseRelationship"]:
        gc = GraphConnection()
        result = gc.match_relationships(cls, limit, skip)

        return result

    @classmethod
    def get_count(cls):
        gc = GraphConnection()
        cypher = (
            f"MATCH (n)-[r:{cls.__relationshiptype__}]->(o) RETURN COUNT(DISTINCT r)"
        )
        result = gc.evaluate_query_single(cypher)
        return result

    def _prep_dump_dict(self, dumped_model: dict, exclude_node_props: True) -> dict:
        if exclude_node_props is True:
            dumped_model["source"] = self.source.get_pp()
            dumped_model["SOURCE_LABEL"] = self.source.__primarylabel__
            dumped_model["target"] = self.target.get_pp()
            dumped_model["TARGET_LABEL"] = self.target.__primarylabel__

        else:
            dumped_model["source"]["LABEL"] = self.source.__primarylabel__
            dumped_model["source"]["PK"] = self.source.get_pp()

            dumped_model["target"]["LABEL"] = self.target.__primarylabel__
            dumped_model["target"]["PK"] = self.target.get_pp()

        dumped_model["RELATIONSHIP_TYPE"] = self.__relationshiptype__

        return dumped_model

    def neontology_dump(
        self,
        exclude_node_props: bool = True,
        exclude: Optional[set] = None,
        exclude_none: bool = True,
        **kwargs,
    ) -> dict:
        dumped_model = self.model_dump(
            exclude_none=exclude_none, exclude=exclude, **kwargs
        )

        return self._prep_dump_dict(dumped_model, exclude_node_props)

    def neontology_dump_json(
        self,
        exclude_node_props: bool = True,
        exclude: Optional[set] = None,
        exclude_none: bool = True,
        **kwargs,
    ) -> str:
        # pydantic converts values to be json serializable, make use of this first
        original_json = self.model_dump_json(
            exclude_none=exclude_none, exclude=exclude, **kwargs
        )
        model_dict = json.loads(original_json)

        return json.dumps(self._prep_dump_dict(model_dict, exclude_node_props))

    @classmethod
    def neontology_schema(
        cls,
        source_labels: Optional[List[str]] = None,
        target_labels: Optional[List[str]] = None,
    ) -> RelationshipSchema:
        schema_properties = []
        rel_type = cls.__relationshiptype__

        if not rel_type:
            raise ValueError("Relationship doesn't have a relationship type.")

        for field_name, field_props in cls.model_fields.items():
            if field_name in ["source", "target"]:
                continue

            field_type = extract_type_mapping(
                field_props.annotation, show_optional=True
            )

            required_field = field_props.is_required()

            rel_prop = SchemaProperty(
                type_annotation=field_type,
                name=field_name,
                required=required_field,
            )

            if required_field is True:
                schema_properties.insert(0, rel_prop)

            else:
                schema_properties.append(rel_prop)

        source_type = cls.model_fields["source"].annotation
        target_type = cls.model_fields["target"].annotation

        if not target_labels:
            if (
                hasattr(target_type, "__primarylabel__")
                and target_type.__primarylabel__
            ):
                target_labels = [target_type.__primarylabel__]

        if not source_labels:
            if (
                hasattr(source_type, "__primarylabel__")
                and source_type.__primarylabel__
            ):
                source_labels = [source_type.__primarylabel__]

        if not source_labels or not target_labels:
            from neontology.utils import get_node_types

            if not target_labels:
                # return concrete subclasses of the abstract node class given
                retrieved_node_types = get_node_types(target_type)
                target_labels = list(retrieved_node_types.keys())

            if not source_labels:
                # return concrete subclasses of the abstract node class given
                retrieved_node_types = get_node_types(source_type)
                source_labels = list(retrieved_node_types.keys())

        schema = RelationshipSchema(
            name=rel_type,
            relationship_type=rel_type,
            properties=schema_properties,
            target_labels=target_labels,
            source_labels=source_labels,
        )

        return schema


class RelationshipTypeData(BaseModel):
    relationship_class: type[BaseRelationship]
    source_class: type[BaseNode]
    target_class: type[BaseNode]
    all_source_classes: List[type[BaseNode]]
    all_target_classes: List[type[BaseNode]]
