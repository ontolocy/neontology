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

        params = self.neo4j_dict()

        all_props = self.neo4j_dict()

        pp_value = all_props.pop(self.__primaryproperty__)

        params = {"pp": pp_value, "all_props": all_props}

        all_labels = [self.__primarylabel__] + self.__secondarylabels__

        cypher = f"""
        CREATE (n:{":".join(all_labels)} {{ {self.__primaryproperty__}: $pp }})
        SET n += $all_props
        RETURN n
        """
        graph = GraphConnection()
        result = graph.cypher_write_single(cypher, params)
        result_node = self.__class__(**dict(result["n"]))
        return result_node

    def merge(self) -> None:
        """Merge this node into the graph."""

        params = self._get_merge_parameters()

        all_labels = [self.__primarylabel__] + self.__secondarylabels__

        cypher = f"""
        MERGE (n:{":".join(all_labels)} {{ {self.__primaryproperty__}: $pp }})
        ON MATCH SET n += $set_on_match
        ON CREATE SET n += $set_on_create
        SET n += $always_set
        RETURN n
        """

        graph = GraphConnection()
        result = graph.cypher_write_single(cypher, params)

        result_node = self.__class__(**dict(result["n"]))

        return result_node

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

        cypher = f"""
        UNWIND $node_list AS node
        create (n:{":".join(all_labels)} {{{cls.__primaryproperty__}: node.pp}})
        SET n = node.props
        RETURN n
        """

        graph = GraphConnection()
        results = graph.cypher_write_many(
            cypher=cypher, params={"node_list": node_list}
        )

        matched_nodes = [cls(**dict(x["n"])) for x in results]

        return matched_nodes

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

        cypher = f"""
        UNWIND $node_list AS node
        MERGE (n:{":".join(all_labels)} {{{cls.__primaryproperty__}: node.pp}})
        ON MATCH SET n += node.set_on_match
        ON CREATE SET n += node.set_on_create
        SET n += node.always_set
        RETURN n
        """

        graph = GraphConnection()
        results = graph.cypher_write_many(
            cypher=cypher, params={"node_list": node_list}
        )

        matched_nodes = [cls(**dict(x["n"])) for x in results]

        return matched_nodes

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

        if deduplicate is True:
            # we don't wan't to waste time attempting to merge identical records
            unique_df = input_df.drop_duplicates(ignore_index=True).copy()
        else:
            unique_df = input_df

        records = unique_df.to_dict(orient="records")

        unique_df["generated_nodes"] = pd.Series(cls.merge_records(records))

        # now we need to get the mapping from unique id to primary property
        # so that we can return the data in the same shape it was received
        input_df.insert(0, "ontolocy_merging_order", range(0, len(input_df)))
        merge_cols = list(input_df.columns)
        merge_cols.remove("ontolocy_merging_order")
        output_df = input_df.merge(
            unique_df,
            how="inner",
            on=merge_cols,
        ).sort_values("ontolocy_merging_order", ignore_index=True)

        return output_df.generated_nodes

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

        graph = GraphConnection()

        result = graph.cypher_read(cypher, params)

        if result:
            return cls(**dict(result["n"]))

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

        cypher = f"""
        MATCH (n:{cls.__primarylabel__})
        WHERE n.{cls.__primaryproperty__} = $pp
        DETACH DELETE n
        """

        params = {"pp": pp}

        graph = GraphConnection()

        graph.cypher_write(cypher, params)

    @classmethod
    def match_nodes(cls: Type[B], limit: int = 100, skip: int = 0) -> List[B]:
        """Get nodes of this type from the database.

        Run a MATCH cypher query to retrieve any Nodes with the label of this class.

        Args:
            limit (int, optional): Maximum number of results to return. Defaults to 100.
            skip (int, optional): Skip through this many results (for pagination). Defaults to 0.

        Returns:
            Optional[List[B]]: A list of node instances.
        """

        cypher = f"""
        MATCH(n:{cls.__primarylabel__})
        RETURN n{{.*}}
        ORDER BY n.created DESC
        SKIP $skip
        LIMIT $limit
        """

        params = {"skip": skip, "limit": limit}

        graph = GraphConnection()
        records = graph.cypher_read_many(cypher, params)

        nodes = [cls(**dict(x["n"])) for x in records]

        return nodes
