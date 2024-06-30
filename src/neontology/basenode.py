import functools
import warnings
from typing import Any, Callable, ClassVar, Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd
from pydantic import ValidationError, model_validator

from .commonmodel import CommonModel
from .gql import GQLIdentifier, gql_identifier_adapter, int_adapter
from .graphconnection import GraphConnection


def _prepare_related_query(
    node: "BaseNode", wrapped_function: Callable, *args: Any, **kwargs: Any
) -> Tuple[str, dict]:
    try:
        query, params = wrapped_function(node, *args, **kwargs)
    except ValueError:
        query = wrapped_function(node, *args, **kwargs)

        # if the function doesn't pass params, they may be taken from user provided parameters
        params = {**kwargs}

    # make it easy to match on this specific node
    this_node = f"(ThisNode:{node.__primarylabel__} {{{node.__primaryproperty__}: $_neontology_pp}})"
    params["_neontology_pp"] = node.get_primary_property_value()
    new_query = query.replace("(#ThisNode)", this_node)

    return new_query, params


def related_property(f: Callable) -> Callable:
    """Decorator to wrap functions on BaseNode subclasses and return a single result."""

    @functools.wraps(f)
    def wrapper(self: "BaseNode", *args: Any, **kwargs: Any) -> Optional[Any]:
        new_query, params = _prepare_related_query(self, f, *args, **kwargs)

        gc = GraphConnection()
        result = gc.evaluate_query_single(new_query, params)

        return result

    return wrapper


def related_nodes(f: Callable) -> Callable:
    """Decorator to wrap functions on BaseNode subclasses and return a list of nodes."""

    @functools.wraps(f)
    def wrapper(self: "BaseNode", *args: Any, **kwargs: Any) -> List["BaseNode"]:
        new_query, params = _prepare_related_query(self, f, *args, **kwargs)

        gc = GraphConnection()
        result = gc.evaluate_query(new_query, params)

        return result.nodes

    return wrapper


class BaseNode(CommonModel):  # pyre-ignore[13]
    __primaryproperty__: ClassVar[GQLIdentifier]
    __primarylabel__: ClassVar[Optional[GQLIdentifier]]
    __secondarylabels__: ClassVar[List[GQLIdentifier]] = []

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
            "pp": self.engine_dict()[self.__primaryproperty__],
            "always_set": self._get_prop_values(self._always_set),
            "set_on_match": self._get_prop_values(self._set_on_match),
            "set_on_create": self._get_prop_values(self._set_on_create),
        }

        return params

    @model_validator(mode="after")
    def validate_identifiers(self) -> "BaseNode":
        try:
            gql_identifier_adapter.validate_strings(self.__primarylabel__)

        except AttributeError:
            pass
        except ValidationError:
            warnings.warn(
                (
                    "Primary Label should contain only alphanumeric characters and underscores."
                    " It should begin with an alphabetic character."
                )
            )

        try:
            gql_identifier_adapter.validate_strings(self.__primaryproperty__)
        except AttributeError:
            pass
        except ValidationError:
            warnings.warn(
                (
                    "Primary Property should contain only alphanumeric characters and underscores."
                    " It should begin with an alphabetic character."
                )
            )

        return self

    def get_primary_property_value(self) -> Union[str, int]:
        return self._get_merge_parameters()["pp"]

    def create(self) -> "BaseNode":
        """Create this node in the graph."""

        all_props = self.engine_dict()

        pp_value = all_props.pop(self.__primaryproperty__)

        node_details = [{"pp": pp_value, "props": all_props}]

        all_labels = [self.__primarylabel__] + self.__secondarylabels__

        pp_key = self.__primaryproperty__

        gc = GraphConnection()

        results = gc.create_nodes(all_labels, pp_key, node_details, self.__class__)

        return results[0]

    def merge(self) -> List["BaseNode"]:
        """Merge this node into the graph."""

        node_list = [self._get_merge_parameters()]

        all_labels = [self.__primarylabel__] + self.__secondarylabels__

        pp_key = self.__primaryproperty__

        gc = GraphConnection()

        results = gc.merge_nodes(all_labels, pp_key, node_list, self.__class__)

        return results

    @classmethod
    def create_nodes(cls, nodes: List["BaseNode"]) -> List["BaseNode"]:
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
            {"props": x.engine_dict(), "pp": x.engine_dict()[cls.__primaryproperty__]}
            for x in nodes
        ]

        all_labels = [cls.__primarylabel__] + cls.__secondarylabels__
        pp_key = cls.__primaryproperty__

        gc = GraphConnection()

        results = gc.create_nodes(all_labels, pp_key, node_list, cls)

        return results

    @classmethod
    def merge_nodes(cls, nodes: List["BaseNode"]) -> List["BaseNode"]:
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
    def merge_records(cls, records: dict) -> List["BaseNode"]:
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
    def merge_df(cls, df: pd.DataFrame, deduplicate: bool = True) -> pd.Series:
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
    def match(cls, pp: str) -> Optional["BaseNode"]:
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

        if label is None:
            raise ValueError("Cannot delete nodes without a primary label.")

        pp_key = cls.__primaryproperty__
        pp_value = pp

        gc = GraphConnection()

        gc.delete_nodes(label, pp_key, [pp_value])

    @classmethod
    def match_nodes(
        cls, limit: Optional[int] = None, skip: Optional[int] = None
    ) -> List["BaseNode"]:
        """Get nodes of this type from the database.

        Run a MATCH cypher query to retrieve any Nodes with the label of this class.

        Args:
            limit (int, optional): Maximum number of results to return. Defaults to None.
            skip (int, optional): Skip through this many results (for pagination). Defaults to None.

        Returns:
            Optional[List[B]]: A list of node instances.
        """

        gc = GraphConnection()
        result = gc.match_nodes(cls, limit, skip)

        return result

    @related_nodes
    def get_related_nodes(
        self,
        relationship_types: list = [],
        relationship_properties: Optional[dict] = None,
        target_label: Optional[str] = None,
        outgoing: bool = True,
        incoming: bool = False,
        depth: Optional[tuple] = None,
        limit: Optional[int] = None,
        skip: Optional[int] = None,
        distinct: bool = False,
    ) -> tuple:
        if target_label:
            target = f"o:{gql_identifier_adapter.validate_strings(target_label)}"
        else:
            target = "o"

        if relationship_types:
            rel_type_match = "r:" + "|".join(
                [gql_identifier_adapter.validate_strings(x) for x in relationship_types]
            )

        else:
            rel_type_match = ""

        if relationship_properties:
            rel_prop_match = (
                "{"
                + ", ".join(
                    [
                        f"{gql_identifier_adapter.validate_strings(x)}: ${x}"
                        for x in relationship_properties
                    ]
                )
                + "}"
            )

            pass_on_params = dict(relationship_properties)

        else:
            rel_prop_match = ""
            pass_on_params = {}

        if outgoing and incoming:
            out_dir = "-"
            in_dir = "-"

        elif outgoing:
            out_dir = "->"
            in_dir = "-"

        elif not outgoing and not incoming:
            raise ValueError("Must specify at least one of incoming or outgoing.")

        else:
            out_dir = "-"
            in_dir = "<-"

        if depth:
            min_depth, max_depth = depth
            if not isinstance(min_depth, int) or not isinstance(max_depth, int):
                raise ValueError("Depth values must be integers")
            rel_depth = f"*{min_depth}..{max_depth}"
        else:
            rel_depth = ""

        if distinct:
            return_distinct = "DISTINCT"

        else:
            return_distinct = ""

        query = f"""
        MATCH (#ThisNode){in_dir}[{rel_type_match}{rel_depth} {rel_prop_match}]{out_dir}({target})
        RETURN {return_distinct} o
        """

        if skip:
            query += f" SKIP {int_adapter.validate_python(skip)} "

        if limit:
            if not isinstance(limit, int):
                raise ValueError("limit value not an integer")
            query += f" LIMIT {int_adapter.validate_python(limit)} "

        return query, pass_on_params
