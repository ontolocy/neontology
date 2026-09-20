import itertools
import json
import warnings
from functools import cached_property
from typing import TYPE_CHECKING, Any, ClassVar, Optional, TypeVar

from pydantic import BaseModel, PrivateAttr, ValidationError, computed_field, model_validator

from neontology.graphconnection import GraphConnection

from .basenode import BaseNode
from .commonmodel import CommonModel
from .gql import gql_identifier_adapter
from .optional_deps import require_pandas
from .registry import registry

if TYPE_CHECKING:
    import pandas as pd

    from .schema import RelationshipSchema


R = TypeVar("R", bound="BaseRelationship")


class BaseRelationship(CommonModel):  # pyre-ignore[13]
    source: BaseNode
    target: BaseNode

    __relationshiptype__: ClassVar[Optional[str]] = None

    _merge_on: list[str] = PrivateAttr()  # what relationship properties should we merge on

    @classmethod
    def _is_abstract(cls) -> bool:
        """Whether this is an abstract relationship, never written to the graph.

        Abstract relationships exist to share properties with subclasses.

        Returns:
            bool: True if this class has no relationship type.
        """
        return getattr(cls, "__relationshiptype__", None) is None

    @classmethod
    def __pydantic_init_subclass__(cls, **kwargs: Any) -> None:
        """Register this relationship class as it is defined.

        Source and target are not resolved here: a relationship may be defined before
        the node classes it points at, so the registry resolves them on demand instead.

        Args:
            **kwargs (Any): class keyword arguments, passed through to pydantic.

        Raises:
            TypeError: if a property is tagged `index` or `unique`, which only node
                properties can be.
        """
        super().__pydantic_init_subclass__(**kwargs)

        # read from the fields, as the JSON Schema cannot be generated until any forward
        # reference to a source or target class resolves
        tagged = [
            f"{cls.__name__}.{name}"
            for name, field in cls.model_fields.items()
            if isinstance(field.json_schema_extra, dict)
            and (field.json_schema_extra.get("index") is True or field.json_schema_extra.get("unique") is True)
        ]

        if tagged:
            raise TypeError(
                f"Cannot tag {', '.join(tagged)} as index or unique: Neontology only indexes and constrains"
                " node properties. Remove the tag, or create the index or constraint on the relationship yourself."
            )

        registry.register_relationship(cls)

    def __init__(self, **data: dict):
        super().__init__(**data)

        # we can define 'abstract' relationships which don't have a label
        # these are to provide common properties to be used by subclassed relationships
        # but shouldn't be put in the graph or even instantiated
        if self._is_abstract():
            raise NotImplementedError(
                f"{type(self).__name__} has no __relationshiptype__, so it is an abstract"
                " relationship: it exists to share properties with subclasses and is never"
                " written to the graph. Give it a __relationshiptype__ to use it directly."
            )

    @classmethod
    def _set_prop_usage(cls) -> None:
        """Set the properties that are used by Neontology for specific purposes.

        This method initializes the class attributes `_merge_on` based on the model's JSON schema.
        It retrieves properties that are marked for merging on, which are used to determine how relationships
        should be merged into the database.
        It also calls the superclass method to set other common property usages.
        """
        super()._set_prop_usage()
        cls._merge_on = cls._get_prop_usage("merge_on")

    @model_validator(mode="after")
    def validate_identifiers(self) -> "BaseRelationship":
        """Validate the relationship type identifier.

        This method checks that the relationship type identifier contains only alphanumeric characters and underscores,
        and that it begins with an alphabetic character.
        If the validation fails, it raises a warning.

        Returns:
            BaseRelationship: The instance of the relationship after validation.
        """
        # as for abstract nodes, a missing relationship type is deliberate rather than
        # malformed, so it is not reported as a bad identifier
        if not self._is_abstract():
            try:
                gql_identifier_adapter.validate_strings(self.__relationshiptype__)
            except ValidationError:
                warnings.warn(
                    (
                        "Relationship type should contain only alphanumeric characters and underscores."
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
        return cls.__relationshiptype__

    def _get_merge_parameters(self, source_prop: str, target_prop: str) -> dict[str, Any]:
        """Get the parameters to use for merging this relationship.

        This method retrieves the properties of the source and target nodes,
        as well as the properties defined in the relationship itself that should be merged.
        It constructs a dictionary of parameters that can be used to merge the relationship
        into the database.

        Args:
            source_prop (str): The property of the source node to use for merging.
            target_prop (str): The property of the target node to use for merging.

        Returns:
            dict[str, Any]: A dictionary of parameters to use for merging the relationship.
        """
        exclusions = {"source", "target"}

        params = self._get_merge_parameters_common(exclude=exclusions)
        # get all the properties
        all_props = params.pop("all_props")

        # merge_props properties will be referenced individually with kwargs
        merge_props = {k: all_props[k] for k in self._merge_on}

        source_prop = self.source.model_dump()[source_prop]
        target_prop = self.target.model_dump()[target_prop]

        params.update(
            {
                "source_prop": source_prop,
                "target_prop": target_prop,
                **merge_props,
            }
        )

        return params

    def merge(
        self,
    ) -> None:
        """Merge this relationship into the database."""
        source_label = self.source.__primarylabel__
        target_label = self.target.__primarylabel__

        if not source_label or not target_label:
            raise ValueError("Source and target Nodes must have a defined primary label for creating a relationship.")

        source_prop = self.source.__primaryproperty__
        target_prop = self.target.__primaryproperty__

        rel_props = self._get_merge_parameters(source_prop=source_prop, target_prop=target_prop)

        merge_on_props = self._merge_on

        rel_type = self.get_relationship_type()

        if not rel_type:
            raise ValueError("Realtionship must have a defined relationship type for creating a relationship.")

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
        cls: type[R],
        rels: list[R],
        source_prop: Optional[str] = None,
        target_prop: Optional[str] = None,
    ) -> None:
        """Merge multiple relationships (of this type) into the database.

        Sometimes the source and target label may be ambiguous (e.g. where we have subclassed nodes)
            In this case you can explicitly pass in the relevant types

        Sometimes we want to match nodes on a property which isn't the primary property,
        so we can specify what property to use.

        Args:
            cls (type[R]): this class
            rels (list[R]): a list of relationships which are instances of this class
            source_prop (Optional[str]): explicitly specify the property to use for the source node
                if None, will use the primary property of the source node class
            target_prop (Optional[str]): explicitly specify the property to use for the target node
                if None, will use the primary property of the target node class

        """
        # define the properties to merge on
        merge_on_props = cls._get_prop_usage("merge_on")

        # sources and targets could have different primary labels
        # to operate efficiently, we group like source and targets for batch creation of relationships
        # sorted first, because groupby only groups runs of adjacent items, and relationships
        # of mixed types arrive interleaved
        def _group_key(rel: R) -> tuple[type, type]:
            return (rel.source.__class__, rel.target.__class__)

        ordered_rels = sorted(rels, key=lambda x: (x.source.__class__.__name__, x.target.__class__.__name__))

        grouped_rels = itertools.groupby(ordered_rels, _group_key)

        for node_clases, common_rels in grouped_rels:
            src_class = node_clases[0]
            tgt_class = node_clases[1]

            source_label = src_class.__primarylabel__
            target_label = tgt_class.__primarylabel__

            # resolved per group rather than assigned to the arguments, which would carry
            # the first group's property names into every group after it
            group_source_prop = source_prop if source_prop is not None else src_class.__primaryproperty__
            group_target_prop = target_prop if target_prop is not None else tgt_class.__primaryproperty__

            if not source_label or not target_label:
                raise ValueError("Source and target Nodes must have a defined primary label to create a relationship.")

            rel_type = cls.get_relationship_type()

            if not rel_type:
                raise ValueError("Relationship must have a defined relationship type for creating a relationship.")

            rel_list: list[dict[str, Any]] = [
                x._get_merge_parameters(group_source_prop, group_target_prop) for x in common_rels
            ]

            gc = GraphConnection()

            gc.merge_relationships(
                source_label,
                target_label,
                group_source_prop,
                group_target_prop,
                rel_type,
                merge_on_props,
                rel_list,
            )

    @classmethod
    def merge_records(
        cls: type[R],
        records: list[dict[str, Any]],
        source_type: Optional[type[BaseNode]] = None,
        target_type: Optional[type[BaseNode]] = None,
        source_prop: Optional[str] = None,
        target_prop: Optional[str] = None,
    ) -> None:
        """Take a list of dictionaries and use them to merge in relationships in the graph.

        Sometimes, a relationship can accept nodes which subclass a particular node type.
            In these instances, it may be necessary to explicitly state what type of node should be used.

        Each record should have a source and target key where the value is the primary property
            value of the respective nodes.

        Args:
            records (list[dict[str, Any]]): a list of dictionaries used to populate relationships
            source_type: explicitly state the class to use for source node
            target_type: explicitly state the class to use for target node
            source_prop: explicitly state the property to use for the source node
            target_prop: explicitly state the property to use for the target node

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

            hydrated["source"] = source_type.model_construct(**{source_prop: record["source"]})
            hydrated["target"] = target_type.model_construct(**{target_prop: record["target"]})

            hydrated_list.append(cls(**hydrated))

        cls.merge_relationships(
            hydrated_list,
            source_prop=source_prop,
            target_prop=target_prop,
        )

    @classmethod
    def merge_df(
        cls: type[R],
        df: "pd.DataFrame",
        source_type: Optional[type[BaseNode]] = None,
        target_type: Optional[type[BaseNode]] = None,
        source_prop: Optional[str] = None,
        target_prop: Optional[str] = None,
    ) -> None:
        """Merge in relationships based on data in a pandas data frame.

        Expects columns named 'source' and 'target' with the primary property value
            for the source and target nodes.

        Then additional fields should have a corresponding column.

        Args:
            df (pd.DataFrame): pandas dataframe where each row represents a relationship to merge.
            source_type (Optional[type[BaseNode]]): The class to use for the source node.
            target_type (Optional[type[BaseNode]]): The class to use for the target node.
            source_prop (Optional[str]): The property to use for the source node.
            target_prop (Optional[str]): The property to use for the target node.
        """
        # raises a helpful ImportError if the pandas extra is not installed
        require_pandas()

        if df.empty is False:
            # see the note in BaseNode.merge_df: casting to object first is what
            # makes the None replacement stick across dtypes
            cleaned_df = df.astype(object).where(df.notna(), None)
            records = cleaned_df.to_dict(orient="records")
            cls.merge_records(
                records,
                source_type=source_type,
                source_prop=source_prop,
                target_type=target_type,
                target_prop=target_prop,
            )

    @classmethod
    def match_relationships(cls, limit: Optional[int] = None, skip: Optional[int] = None) -> list["BaseRelationship"]:
        """Match relationships of this type in the graph.

        Constructs a Cypher query to match relationships of the specified type in the graph database.
        It uses the GraphConnection class to execute the query and return a list of relationships.

        Args:
            cls (type[R]): The class of the relationship to match.
            limit (Optional[int]): The maximum number of relationships to return.
            skip (Optional[int]): The number of relationships to skip before returning results.

        Returns:
            list[BaseRelationship]: A list of relationships of the specified type.
        """
        gc = GraphConnection()
        result = gc.match_relationships(cls, limit, skip)

        return result

    @classmethod
    def get_count(cls):
        """Get the count of relationships of this type in the graph.

        Constructs a Cypher query to count the number of distinct relationships
            of the specified type in the graph database.
        It uses the GraphConnection class to execute the query and return the count.

        Returns:
            int: The count of distinct relationships of the specified type.
        """
        gc = GraphConnection()
        cypher = f"MATCH (n)-[r:{cls.__relationshiptype__}]->(o) RETURN COUNT(r)"
        result = gc.evaluate_query_single(cypher)
        return result

    def _prep_dump_dict(self, dumped_model: dict, exclude_node_props: bool = True) -> dict:
        """Prepare the dumped model dictionary for Neontology.

        This method modifies the dumped model dictionary to include additional metadata
        such as the source and target node labels and the relationship type.

        Args:
            dumped_model (dict): The dumped model dictionary.
            exclude_node_props (bool): Whether to exclude the source and target node properties.

        Returns:
            dict: The modified dumped model dictionary.
        """
        if exclude_node_props is True:
            # the content format names these keys in uppercase, as it does every key
            # which says how to build the graph rather than being a property
            dumped_model.pop("source", None)
            dumped_model.pop("target", None)

            dumped_model["SOURCE"] = self.source.get_pp()
            dumped_model["SOURCE_LABEL"] = self.source.__primarylabel__
            dumped_model["TARGET"] = self.target.get_pp()
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
        """Dump the relationship as a dictionary.

        The generated dictionary can be used to create a relationship in Neontology.
        It includes additional metadata such as the source and target node labels and the relationship type.

        Args:
            exclude_node_props (bool): Whether to exclude the source and target node properties.
                Defaults to True.
            exclude (Optional[set]): A set of properties to exclude from the dump.
                Defaults to None.
            exclude_none (bool): Whether to exclude properties with None values.
                Defaults to True.
            **kwargs: Additional keyword arguments to pass to the model_dump method.

        Returns:
            dict: A dictionary representation of the relationship.
        """
        dumped_model = self.model_dump(exclude_none=exclude_none, exclude=exclude, **kwargs)

        return self._prep_dump_dict(dumped_model, exclude_node_props)

    def neontology_dump_json(
        self,
        exclude_node_props: bool = True,
        exclude: Optional[set] = None,
        exclude_none: bool = True,
        **kwargs,
    ) -> str:
        """Dump the relationship as a JSON string.

        The generated JSON can be imported with Neontology. It includes additional metadata
        such as the source and target node labels and the relationship type.

        Args:
            exclude_node_props (bool): Whether to exclude the source and target node properties.
                Defaults to True.
            exclude (Optional[set]): A set of properties to exclude from the dump.
                Defaults to None.
            exclude_none (bool): Whether to exclude properties with None values.
                Defaults to True.
            **kwargs: Additional keyword arguments to pass to the model_dump method.

        Returns:
            str: A JSON string representation of the relationship.
        """
        # pydantic converts values to be json serializable, make use of this first
        original_json = self.model_dump_json(exclude_none=exclude_none, exclude=exclude, **kwargs)
        model_dict = json.loads(original_json)

        return json.dumps(self._prep_dump_dict(model_dict, exclude_node_props))

    @classmethod
    def neontology_schema(cls) -> "RelationshipSchema":
        """Describe this relationship class: its type, the nodes at each end, and its properties.

        `neontology.get_ontology_schema()` describes every model at once.

        Returns:
            RelationshipSchema: the description.

        Raises:
            ValueError: if the class is abstract, with no relationship type.
        """
        # imported here: the schema module builds on this one
        from .schema import _relationship_schema

        return _relationship_schema(cls)


class RelationshipTypeData(BaseModel):
    relationship_class: type[BaseRelationship]
    source_class: type[BaseNode]
    target_class: type[BaseNode]

    # The subclasses of the source and target are worked out on demand rather than when
    # this is built. Type discovery constructs one of these per relationship on every
    # query, and nothing on that path reads them - walking the node hierarchy twice per
    # relationship to populate them was most of the cost of the walk.

    @computed_field  # type: ignore[prop-decorator]
    @cached_property
    def all_source_classes(self) -> list[type[BaseNode]]:
        """The source class and every class which inherits from it."""
        from .utils import get_node_types

        return list(get_node_types(self.source_class).values())

    @computed_field  # type: ignore[prop-decorator]
    @cached_property
    def all_target_classes(self) -> list[type[BaseNode]]:
        """The target class and every class which inherits from it."""
        from .utils import get_node_types

        return list(get_node_types(self.target_class).values())
