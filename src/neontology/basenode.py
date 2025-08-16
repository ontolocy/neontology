import functools
import json
import warnings
from typing import Any, Callable, ClassVar, Optional, TypeVar, Union

import pandas as pd
from pydantic import ValidationError, model_validator
from typing_extensions import ParamSpec, Self

from .commonmodel import CommonModel
from .gql import gql_identifier_adapter, int_adapter
from .graphconnection import GraphConnection
from .result import NeontologyResult
from .schema_utils import NodeSchema, SchemaProperty, extract_type_mapping

P = ParamSpec("P")
R = TypeVar("R")


def _find_this_node(query, params, node):
    this_node = f"(ThisNode:{node.__primarylabel__} {{{node.__primaryproperty__}: $_neontology_pp}})"
    params["_neontology_pp"] = node.get_pp()
    new_query = query.replace("(#ThisNode)", this_node)

    return new_query, params


def _prepare_related_query(node: "BaseNode", wrapped_function: Callable, *args: Any, **kwargs: Any) -> tuple[str, dict]:
    try:
        query, params = wrapped_function(node, *args, **kwargs)
    except ValueError:
        query = wrapped_function(node, *args, **kwargs)

        # if the function doesn't pass params, they may be taken from user provided parameters
        params = {**kwargs}

    # make it easy to match on this specific node
    if "(#ThisNode)" in query:
        new_query, params = _find_this_node(query, params, node)

    else:
        new_query = query

    return new_query, params


def related_property(f: Callable[P, R]) -> Callable:
    """Decorator to wrap functions on BaseNode subclasses and return a single result."""

    @functools.wraps(f)
    def wrapper(self: "BaseNode", *args: P.args, **kwargs: P.kwargs) -> Optional[Any]:
        new_query, params = _prepare_related_query(self, f, *args, **kwargs)

        gc = GraphConnection()
        result = gc.evaluate_query_single(new_query, params)

        return result

    wrapper.neontology_related_prop = True  # type: ignore

    return wrapper


def related_nodes(f: Callable[P, R]) -> Callable:
    """Decorator to wrap functions on BaseNode subclasses and return a list of nodes."""

    @functools.wraps(f)
    def wrapper(self: "BaseNode", *args: P.args, **kwargs: P.kwargs) -> list["BaseNode"]:
        new_query, params = _prepare_related_query(self, f, *args, **kwargs)

        gc = GraphConnection()
        result = gc.evaluate_query(new_query, params)

        return result.nodes

    wrapper.neontology_related_nodes = True  # type: ignore

    return wrapper


class BaseNode(CommonModel):  # pyre-ignore[13]
    __primaryproperty__: ClassVar[str]
    __primarylabel__: ClassVar[Optional[str]]
    __secondarylabels__: ClassVar[list[str]] = []

    def __init__(self, **data: dict):
        super().__init__(**data)

        # we can define 'abstract' nodes which don't have a label
        # these are to provide common properties to be used by subclassed nodes
        # but shouldn't be put in the graph or even instantiated
        if self.__primarylabel__ is None:
            raise NotImplementedError("Nodes to be used in the graph must define a primary label.")

    def __str__(self) -> str:
        """String representation of the node, showing the primary property value by default."""
        return str(self.get_pp())

    def _get_merge_parameters(self) -> dict[str, Any]:
        """Get the parameters defined for merging on when merging into the graph.

        Returns:
        dict[str, Any]: a dictionary of key/value pairs.
        """
        params = self._get_merge_parameters_common()
        # get all the properties
        all_props = params.pop("all_props")

        params.update(
            {
                "pp": all_props[self.__primaryproperty__],
            }
        )

        return params

    @classmethod
    def get_related_node_methods(cls) -> dict:
        """Gather all methods tagged with `related_nodes` decorator.

        Returns:
            dict: A dictionary of method names and their corresponding methods.
        """
        related_node_attributes = {
            name: getattr(cls, name)
            # get all attributes, including methods, properties, and builtins
            for name in dir(cls)
            # only want methods
            if callable(getattr(cls, name))
            # filter to tagged methods
            and hasattr(getattr(cls, name), "neontology_related_nodes")
        }
        return related_node_attributes

    @classmethod
    def get_related_property_methods(cls) -> dict:
        """Gather all methods tagged with `related_property` decorator.

        Returns:
            dict: A dictionary of method names and their corresponding methods.
        """
        related_prop_attributes = {
            name: getattr(cls, name)
            # get all attributes, including methods, properties, and builtins
            for name in dir(cls)
            # only want methods
            if callable(getattr(cls, name))
            # filter to tagged methods
            and hasattr(getattr(cls, name), "neontology_related_prop")
        }
        return related_prop_attributes

    @model_validator(mode="after")
    def validate_identifiers(self) -> Self:
        """Validate data provided for primary label and primary property."""
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

    def get_pp(self) -> Union[str, int]:
        """Get the primary property value for this node.

        Returns:
            Union[str, int]: The value of the primary property.
        """
        return self._get_merge_parameters()["pp"]

    def get_primary_property_value(self) -> Union[str, int]:
        """Get the primary property value for this node.

        Deprecated: Use `get_pp()` instead.

        Returns:
            Union[str, int]: The value of the primary property.
        """
        warnings.warn(("get_primary_property_value is deprecated, use get_pp instead."))
        return self.get_pp()

    def create(self) -> Self:
        """Create this node in the graph."""
        all_props = self._engine_dict()

        pp_value = all_props.pop(self.__primaryproperty__)

        node_details = [{"pp": pp_value, "props": all_props}]

        all_labels = [self.__primarylabel__] + self.__secondarylabels__

        pp_key = self.__primaryproperty__

        gc = GraphConnection()

        results = gc.create_nodes(all_labels, pp_key, node_details, self.__class__)

        return results[0]

    def merge(self) -> list[Self]:
        """Merge this node into the graph."""
        node_list = [self._get_merge_parameters()]

        all_labels = [self.__primarylabel__] + self.__secondarylabels__

        pp_key = self.__primaryproperty__

        gc = GraphConnection()

        results = gc.merge_nodes(all_labels, pp_key, node_list, self.__class__)

        return results

    @classmethod
    def create_nodes(cls, nodes: list[Self]) -> list[Self]:
        """Create the given nodes in the database.

        Args:
            nodes (list[B]): A list of nodes to create.

        Returns:
            list: A list of the primary property values

        Raises:
            TypeError: Raised if one of the nodes isn't of this type.
        """
        node_list = [{"props": x._engine_dict(), "pp": x._engine_dict()[cls.__primaryproperty__]} for x in nodes]

        all_labels = [cls.__primarylabel__] + cls.__secondarylabels__
        pp_key = cls.__primaryproperty__

        gc = GraphConnection()

        results = gc.create_nodes(all_labels, pp_key, node_list, cls)

        return results

    @classmethod
    def merge_nodes(cls, nodes: list[Self]) -> list[Self]:
        """Merge multiple nodes into the database.

        Args:
            nodes (list[B]): A list of nodes to merge.

        Returns:
            list: A list of the primary property values

        Raises:
            TypeError: Raised if any of the nodes provided don't match this class.
        """
        node_list = [x._get_merge_parameters() for x in nodes]

        all_labels = [cls.__primarylabel__] + cls.__secondarylabels__

        pp_key = cls.__primaryproperty__

        gc = GraphConnection()

        results = gc.merge_nodes(all_labels, pp_key, node_list, cls)

        return results

    @classmethod
    def merge_records(cls, records: list[dict]) -> list[Self]:
        """Take a list of dictionaries and use them to merge in nodes in the graph.

        Each dictionary will be used to merge a node where dictionary key/value pairs
            represent properties to be applied.

        Returns:
            list: A list of the primary property values

        Args:
            records (list[dict[str, Any]]): a list of dictionaries of node properties
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
            deduplicate (bool): If True, deduplicate the dataframe before merging.
                Defaults to True.

        """
        if df.empty is True:
            return pd.Series(dtype=object)

        input_df = df.mask(pd.isna(df), None).copy()

        # create a unique identifier field based on all rows
        # we'll use this later to match up deduplicated rows to the original ordering
        input_df["unique_identifier"] = input_df.astype(str).values.sum(axis=1)

        if deduplicate is True:
            # we don't wan't to waste time attempting to merge identical records
            unique_df = input_df.drop_duplicates(subset="unique_identifier", ignore_index=True).copy()
        else:
            unique_df = input_df

        model_data = unique_df.drop("unique_identifier", axis=1).copy()

        records = model_data.to_dict(orient="records")

        unique_df["generated_nodes"] = pd.Series(cls.merge_records(records))

        # now we need to get the mapping from unique id to primary property
        # so that we can return the data in the same shape it was received
        input_df.insert(0, "ontolocy_merging_order", range(0, len(input_df)))
        output_df = input_df.merge(unique_df, how="outer", on="unique_identifier", suffixes=(None, "_y"))

        ordered_nodes = output_df.sort_values("ontolocy_merging_order", ignore_index=True).generated_nodes.copy()

        return ordered_nodes

    @classmethod
    def match(cls, pp: str) -> Optional[Self]:
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

        result = gc.evaluate_query(cypher, params, node_classes={cls.__primarylabel__: cls})

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
        cls,
        limit: Optional[int] = None,
        skip: Optional[int] = None,
        filters: Optional[dict] = None,
    ) -> list[Self]:
        """Get nodes of this type from the database with optional filtering.

        Run a MATCH cypher query to retrieve any Nodes with the label of this class.

        Args:
            filters (dict, optional): Dictionary of filters using Django-like syntax:
                - {"name": "exact_value"} → exact match (case-sensitive)
                - {"name__icontains": "part"} → case-insensitive contains
                - {"name__exact": "Value"} → exact match (case-sensitive)
                - {"name__iexact": "value"} → exact match (case-insensitive)
                - {"quantity__gt": 100} → greater than
                - {"date__lt": some_date} → less than
                Defaults to None.
            limit (int, optional): Maximum number of results to return. Defaults to None.
            skip (int, optional): Skip through this many results (for pagination). Defaults to None.

        Returns:
            list[Self]: A list of node instances matching the criteria.
        """
        gc = GraphConnection()
        result = gc.match_nodes(cls, limit=limit, skip=skip, filters=filters)
        return result

    def get_related(
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
    ) -> NeontologyResult:
        """Get related nodes based on relationships of this node.

        Args:
            relationship_types (list, optional): List of relationship types to match on.
                Defaults to [].
            relationship_properties (dict, optional): Dictionary of relationship properties to match on.
                Defaults to None.
            target_label (str, optional): Label of the target node to match.
                Defaults to None, which matches any label.
            outgoing (bool, optional): If True, match outgoing relationships.
                Defaults to True.
            incoming (bool, optional): If True, match incoming relationships.
                Defaults to False.
            depth (tuple, optional): A tuple of (min_depth, max_depth) to limit the depth of relationships.
                Defaults to None, which matches any depth.
            limit (int, optional): Maximum number of results to return. Defaults to None.
            skip (int, optional): Skip through this many results (for pagination). Defaults to None.
            distinct (bool, optional): If True, return distinct results. Defaults to False.

        Returns:
            NeontologyResult: A result object containing the nodes and relationships found.

        Raises:
            ValueError: If neither outgoing nor incoming is specified.
        """
        if target_label:
            target = f"o:{gql_identifier_adapter.validate_strings(target_label)}"
        else:
            target = "o"

        if relationship_types:
            rel_type_match = "r:" + "|".join([gql_identifier_adapter.validate_strings(x) for x in relationship_types])

        else:
            rel_type_match = "r"

        if relationship_properties:
            rel_prop_match = (
                "{" + ", ".join([f"{gql_identifier_adapter.validate_strings(x)}: ${x}" for x in relationship_properties]) + "}"
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

            rel_depth = f"*{min_depth}..{max_depth}"
        else:
            rel_depth = ""

        if distinct:
            return_distinct = "DISTINCT"

        else:
            return_distinct = ""

        query = f"""
        MATCH (#ThisNode){in_dir}[{rel_type_match}{rel_depth} {rel_prop_match}]{out_dir}({target})
        RETURN {return_distinct} o, r, ThisNode
        """

        if skip:
            query += f" SKIP {int_adapter.validate_python(skip)} "

        if limit:
            query += f" LIMIT {int_adapter.validate_python(limit)} "

        new_query, params = _find_this_node(query, pass_on_params, self)

        gc = GraphConnection()
        result = gc.evaluate_query(new_query, params)

        return result

    @classmethod
    def get_count(
        cls,
        filters: Optional[dict] = None,
    ) -> int:
        """Get the count of nodes of this type in the graph database with optional filtering.

        Args:
            filters (dict | None): Dictionary of filters using Django-like syntax.

        Returns:
            int: Count of matched nodes.
        """
        gc = GraphConnection()
        return gc.get_count(cls, filters=filters)

    def _prep_dump_dict(self, dumped_model: dict) -> dict:
        dumped_model["LABEL"] = self.__primarylabel__

        return dumped_model

    def neontology_dump(self, exclude: Optional[set] = None, exclude_none: bool = True, **kwargs) -> dict:
        """Dump the model as a dictionary which can be reimported.

        Args:
            exclude (set, optional): A set of fields to exclude from the dump.
                Defaults to None.
            exclude_none (bool, optional): If True, exclude fields with None values.
                Defaults to True.
            **kwargs: Additional keyword arguments to pass to the model_dump method.

        Returns:
            dict: A dictionary representation of the model.
        """
        dumped_model = self.model_dump(exclude_none=exclude_none, exclude=exclude, **kwargs)

        return self._prep_dump_dict(dumped_model)

    def neontology_dump_json(self, exclude: Optional[set] = None, exclude_none: bool = True, **kwargs) -> str:
        """Dump the model as a JSON string which can be reimported.

        Args:
            exclude (set, optional): A set of fields to exclude from the dump.
                Defaults to None.
            exclude_none (bool, optional): If True, exclude fields with None values.
                Defaults to True.
            **kwargs: Additional keyword arguments to pass to the model_dump_json method.

        Returns:
            str: A JSON string representation of the model.
        """
        # pydantic converts values to be json serializable, make use of this first
        original_json = self.model_dump_json(exclude_none=exclude_none, exclude=exclude, **kwargs)
        model_dict = json.loads(original_json)
        model_dict["LABEL"] = self.__primarylabel__
        return json.dumps(self._prep_dump_dict(model_dict))

    @classmethod
    def neontology_schema(cls, include_outgoing_rels: bool = True) -> NodeSchema:
        """Generate a schema for this node class.

        Args:
            include_outgoing_rels (bool, optional): If True, include outgoing relationships in the schema.
                Defaults to True.

        Returns:
            NodeSchema: A schema object representing the node class.

        Raises:
            ValueError: If the node class does not have a primary label defined.
        """
        if not cls.__primarylabel__:
            raise ValueError("Node does not have a primary label defined for generating schema.")

        schema_dict: dict = {}
        schema_dict["label"] = cls.__primarylabel__
        schema_dict["title"] = cls.__name__
        schema_dict["secondary_labels"] = cls.__secondarylabels__

        model_properties: list = []

        for field_name, field_props in cls.model_fields.items():
            field_type = extract_type_mapping(field_props.annotation, show_optional=True)

            node_property = SchemaProperty(
                type_annotation=field_type,
                name=field_name,
                required=field_props.is_required(),
            )

            if field_props.is_required() is True:
                model_properties.insert(0, node_property)

            # put optional fields at the end
            else:
                model_properties.append(node_property)

        schema_dict["properties"] = model_properties
        schema_dict["outgoing_relationships"] = []

        if include_outgoing_rels is False:
            return NodeSchema(**schema_dict)

        else:
            from .utils import get_rels_by_source, get_rels_by_type

            outgoing_rels = get_rels_by_source().get(cls.__primarylabel__, set())
            all_rel_types = get_rels_by_type()

            for rel in outgoing_rels:
                rel_class = all_rel_types[rel].relationship_class

                rel_schema = rel_class.neontology_schema(source_labels=[schema_dict["label"]])

                schema_dict["outgoing_relationships"].append(rel_schema)

        return NodeSchema(**schema_dict)
