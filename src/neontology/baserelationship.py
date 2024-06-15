"""Defines the BaseRelationship class.

The BaseRelationship class is used for creating and matching on relationships in the graph.

    Typical usage example:

    class MyRel(BaseRelationship):

        __relationshiptype__: ClassVar[Optional[str]] = "MY_REL"

        source: SourceNode
        target: TargetNode

    my_rel = MyRel(source=source_node, target=target_node)
    my_rel.merge()

"""

from typing import Any, ClassVar, Dict, List, Optional, Type, TypeVar

import numpy as np
import pandas as pd
from pydantic import PrivateAttr

from neontology.graphconnection import GraphConnection

from .basenode import BaseNode
from .commonmodel import CommonModel

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

        self._merge_on = self._get_prop_usage("merge_on")

        # we can define 'abstract' relationships which don't have a label
        # these are to provide common properties to be used by subclassed relationships
        # but shouldn't be put in the graph or even instantiated
        if self.__relationshiptype__ is None:
            raise NotImplementedError(
                "Relationships to be used in the graph must define a relationship type."
            )

    @classmethod
    def get_relationship_type(cls) -> str:
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

        # these properties will be referenced individually
        merge_props = self._get_prop_values(self._merge_on, exclude=exclusions)

        params = {
            "source_prop": self.source.neo4j_dict()[source_prop],
            "target_prop": self.target.neo4j_dict()[target_prop],
            "always_set": self._get_prop_values(self._always_set, exclude=exclusions),
            "set_on_match": self._get_prop_values(
                self._set_on_match, exclude=exclusions
            ),
            "set_on_create": self._get_prop_values(
                self._set_on_create, exclude=exclusions
            ),
            **merge_props,
        }

        return params

    def merge(
        self,
    ) -> None:
        """Merge this relationship into the database."""
        source_label = self.source.__primarylabel__
        target_label = self.target.__primarylabel__

        source_prop = self.source.__primaryproperty__
        target_prop = self.target.__primaryproperty__

        rel_props = self._get_merge_parameters(
            source_prop=source_prop, target_prop=target_prop
        )

        merge_on_props = self._merge_on

        rel_type = self.get_relationship_type()

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
        source_type: Optional[Type[BaseNode]] = None,
        target_type: Optional[Type[BaseNode]] = None,
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

        if source_type is None:
            source_type = cls.model_fields["source"].annotation

        if target_type is None:
            target_type = cls.model_fields["target"].annotation

        for rel in rels:
            if isinstance(rel, cls) is False:
                raise TypeError("Relationship was incorrect type.")
            if type(rel.source) is not source_type:
                raise TypeError("Received an inappropriate kind of source node.")
            if type(rel.target) is not target_type:
                raise TypeError("Received an inappropriate kind of target node.")

        if source_prop is None:
            source_prop = source_type.__primaryproperty__

        if target_prop is None:
            target_prop = target_type.__primaryproperty__

        source_label = source_type.__primarylabel__
        target_label = target_type.__primarylabel__

        rel_type = cls.get_relationship_type()

        merge_on_props = cls._get_prop_usage("merge_on")

        rel_list: List[Dict[str, Any]] = [
            x._get_merge_parameters(source_prop, target_prop) for x in rels
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

            hydrated_list.append(hydrated)

        rels = [cls(**x) for x in hydrated_list]

        cls.merge_relationships(
            rels,
            source_type=source_type,
            source_prop=source_prop,
            target_type=target_type,
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
