from typing import Any, ClassVar, Dict, List, Optional, Type, TypeVar, Union

import numpy as np
import pandas as pd

from .commonmodel import CommonModel
from .graphconnection import GraphConnection

B = TypeVar("B", bound="BaseNode")


class BaseNode(CommonModel):  # pyre-ignore[13]
    __primaryproperty__: ClassVar[str]
    __primarylabel__: ClassVar[Optional[str]]
    __secondarylabels__: ClassVar[Optional[list]] = []

    def __init__(self, **data: dict):
        super().__init__(**data)

        # we can define 'abstract' nodes which don't have a label
        # these are to provide common properties to be used by subclassed nodes
        # but shouldn't be put in the graph or even instantiated
        if self.__primarylabel__ is None:
            raise NotImplementedError(
                "Nodes to be used in the graph must define a primary label."
            )

    def _get_merge_parameters(self) -> Dict[str, Any]:
        """

        Returns:
            Dict[str, Any]: a dictionary of key/value pairs.
        """

        params = {
            "pp": self.neo4j_dict()[self.__primaryproperty__],
            "always_set": self._get_prop_values(self._always_set),
            "set_on_match": self._get_prop_values(self._set_on_match),
            "set_on_create": self._get_prop_values(self._set_on_create),
        }

        return params

    def get_primary_property_value(self) -> Union[str, int]:
        return self._get_merge_parameters()["pp"]

    def create(self) -> None:
        """Create this node in the graph."""

        all_props = self.neo4j_dict()

        pp_value = all_props.pop(self.__primaryproperty__)

        node_details = [{"pp": pp_value, "props": all_props}]

        all_labels = [self.__primarylabel__] + self.__secondarylabels__

        pp_key = self.__primaryproperty__

        gc = GraphConnection()

        results = gc.create_nodes(all_labels, pp_key, node_details, self.__class__)

        return results[0]

    def merge(self) -> None:
        """Merge this node into the graph."""

        node_list = [self._get_merge_parameters()]

        all_labels = [self.__primarylabel__] + self.__secondarylabels__

        pp_key = self.__primaryproperty__

        gc = GraphConnection()

        results = gc.merge_nodes(all_labels, pp_key, node_list, self.__class__)

        return results

    @classmethod
    def create_nodes(cls: Type[B], nodes: List[B]) -> List[Union[str, int]]:
        """Create the given nodes in the database.

        Args:
            nodes (List[B]): A list of nodes to create.

        Returns:
            list: A list of the primary property values

        Raises:
            TypeError: Raised if one of the nodes isn't of this type.
        """

        for node in nodes:
            if isinstance(node, cls) is False:
                raise TypeError("Node was incorrect type.")

        node_list = [
            {"props": x.neo4j_dict(), "pp": x.neo4j_dict()[cls.__primaryproperty__]}
            for x in nodes
        ]

        all_labels = [cls.__primarylabel__] + cls.__secondarylabels__
        pp_key = cls.__primaryproperty__

        gc = GraphConnection()

        results = gc.create_nodes(all_labels, pp_key, node_list, cls)

        return results

    @classmethod
    def merge_nodes(cls: Type[B], nodes: List[B]) -> List[B]:
        """Merge multiple nodes into the database.

        Args:
            nodes (List[B]): A list of nodes to merge.

        Returns:
            list: A list of the primary property values

        Raises:
            TypeError: Raised if any of the nodes provided don't match this class.
        """

        for node in nodes:
            if isinstance(node, cls) is False:
                raise TypeError("Node was incorrect type.")

        node_list = [x._get_merge_parameters() for x in nodes]

        all_labels = [cls.__primarylabel__] + cls.__secondarylabels__

        pp_key = cls.__primaryproperty__

        gc = GraphConnection()

        results = gc.merge_nodes(all_labels, pp_key, node_list, cls)

        return results

    @classmethod
    def merge_records(cls: Type[B], records: dict) -> List[B]:
        """Take a list of dictionaries and use them to merge in nodes in the graph.

        Each dictionary will be used to merge a node where dictionary key/value pairs
            represent properties to be applied.

        Returns:
            list: A list of the primary property values

        Args:
            records (List[Dict[str, Any]]): a list of dictionaries of node properties
        """

        nodes = [cls(**x) for x in records]

        return cls.merge_nodes(nodes)

    @classmethod
    def merge_df(cls: Type[B], df: pd.DataFrame, deduplicate: bool = True) -> pd.Series:
        """Merge in new nodes based on data in a dataframe.

        The dataframe columns must correspond to the Node properties.

        Returns:
            pd.Series: A list of the primary property values

        Args:
            df (pd.DataFrame): A pandas dataframe of node properties

        """

        if df.empty is True:
            return pd.Series(dtype=object)

        input_df = df.replace([np.nan], None).copy()

        # create a unique identifier field based on all rows
        # we'll use this later to match up deduplicated rows to the original ordering
        input_df["unique_identifier"] = input_df.astype(str).values.sum(axis=1)

        if deduplicate is True:
            # we don't wan't to waste time attempting to merge identical records
            unique_df = input_df.drop_duplicates(
                subset="unique_identifier", ignore_index=True
            ).copy()
        else:
            unique_df = input_df

        model_data = unique_df.drop("unique_identifier", axis=1).copy()

        records = model_data.to_dict(orient="records")

        unique_df["generated_nodes"] = pd.Series(cls.merge_records(records))

        # now we need to get the mapping from unique id to primary property
        # so that we can return the data in the same shape it was received
        input_df.insert(0, "ontolocy_merging_order", range(0, len(input_df)))
        output_df = input_df.merge(
            unique_df, how="outer", on="unique_identifier", suffixes=(None, "_y")
        )

        ordered_nodes = output_df.sort_values(
            "ontolocy_merging_order", ignore_index=True
        ).generated_nodes.copy()

        return ordered_nodes

    @classmethod
    def match(cls: Type[B], pp: str) -> Optional[B]:
        """MATCH a single node of this type with the given primary property.

        Args:
            pp (str): The value of the primary property (pp) to match on.

        Returns:
            Optional[B]: If the node exists, return it as an instance.
        """

        cypher = f"""
        MATCH (n:{cls.__primarylabel__})
        WHERE n.{cls.__primaryproperty__} = $pp
        RETURN n
        """

        params = {"pp": pp}

        gc = GraphConnection()

        result = gc.evaluate_query(
            cypher, params, node_classes={cls.__primarylabel__: cls}
        )

        if result.nodes:
            return result.nodes[0]

        else:
            return None

    @classmethod
    def delete(cls, pp: str) -> None:
        """Delete a node from the graph.

        Match on label and the pp value provided.
        If the node exists, delete it and any relationships it has.

        Args:
            pp (str): Primary property value to match on.
        """

        label = cls.__primarylabel__
        pp_key = cls.__primaryproperty__
        pp_value = pp

        gc = GraphConnection()

        gc.delete_nodes(label, pp_key, [pp_value])

    @classmethod
    def match_nodes(
        cls: Type[B], limit: Optional[int] = None, skip: Optional[int] = None
    ) -> List[B]:
        """Get nodes of this type from the database.

        Run a MATCH cypher query to retrieve any Nodes with the label of this class.

        Args:
            limit (int, optional): Maximum number of results to return. Defaults to None.
            skip (int, optional): Skip through this many results (for pagination). Defaults to None.

        Returns:
            Optional[List[B]]: A list of node instances.
        """

        graph = GraphConnection()
        result = graph.match_nodes(cls, limit, skip)

        return result
