import os
from typing import Any, ClassVar, List, Optional

from dotenv import load_dotenv
from neo4j import GraphDatabase
from neo4j import Result as Neo4jResult
from pydantic import model_validator

from ..gql import gql_identifier_adapter
from ..result import NeontologyResult
from .graphengine import GraphEngineBase, GraphEngineConfig
from .neo4jengine import neo4j_records_to_neontology_records


class MemgraphEngine(GraphEngineBase):
    """
    Note. the memgraph engine is very similar to the Neo4j engine as they
        can both use the same Neo4j driver
    Args:
        Neo4jEngine (_type_): _description_
    """

    def __init__(self, config: "MemgraphConfig") -> None:
        """Initialise connection to the engine

        Args:
            config - Takes a MemgraphConfig object.
        """

        self.driver = GraphDatabase.driver(  # type: ignore
            config.uri,
            auth=(config.username, config.password),
        )

    def verify_connection(self) -> bool:
        try:
            self.driver.verify_connectivity()
            return True
        except:  # noqa: E722
            return False

    def close_connection(self) -> None:
        try:
            self.driver.close()
        except AttributeError:
            pass

    def evaluate_query(
        self,
        cypher: str,
        params: dict = {},
        node_classes: dict = {},
        relationship_classes: dict = {},
    ) -> NeontologyResult:
        result = self.driver.execute_query(cypher, parameters_=params)

        neo4j_records = result.records
        neontology_records, nodes, rels, paths = neo4j_records_to_neontology_records(
            neo4j_records, node_classes, relationship_classes
        )

        return NeontologyResult(
            records_raw=neo4j_records,
            records=neontology_records,
            nodes=nodes,
            relationships=rels,
            paths=paths,
        )

    def evaluate_query_single(self, cypher: str, params: dict = {}) -> Optional[Any]:
        result = self.driver.execute_query(
            cypher, parameters_=params, result_transformer_=Neo4jResult.single
        )

        if result:
            return result.value()

        else:
            return None

    def match_node(self, pp: str, node_class: type["BaseNode"]) -> Optional["BaseNode"]:
        """MATCH a single node of this type with the given primary property.
        Memgraph specific element id function version

        Args:
            pp (str): The value of the primary property (pp) to match on.
            node_class (type[BaseNode]): Class of the node to match

        Returns:
            Optional[B]: If the node exists, return it as an instance.
        """
        element_id_prop_name = getattr(node_class, "__elementidproperty__", None)
        if node_class.__primaryproperty__ == element_id_prop_name:
            match_cypher = "toString(id(n))"
        else:
            match_cypher = f"n.{node_class.__primaryproperty__}"

        cypher = f"""
        MATCH (n:{node_class.__primarylabel__})
        WHERE {match_cypher} = $pp
        RETURN n
        """

        params = {"pp": pp}

        result = self.evaluate_query(
            cypher, params, node_classes={node_class.__primarylabel__: node_class}
        )

        if result.nodes:
            return result.nodes[0]

        else:
            return None

    @staticmethod
    def _where_elementId_cypher() -> str:
        return "toString(id(n)) = $pp"

    def merge_relationships(
        self,
        source_label: str,
        target_label: str,
        source_prop: str,
        target_prop: str,
        rel_type: str,
        merge_on_props: List[str],
        rel_props: List[dict],
        rel_class: type["BaseRelationship"],
    ) -> NeontologyResult:
        """Merge relationships - memgraph specific element id function version"""
        # build a string of properties to merge on "prop_name: $prop_name"
        merge_props = ", ".join(
            [
                f"{gql_identifier_adapter.validate_strings(x)}: rel.{x}"
                for x in merge_on_props
            ]
        )

        if rel_props[0]["source_element_id_prop"] == source_prop:
            source_match_cypher = """toString(id(source))"""
        else:
            source_match_cypher = (
                f"""source.{gql_identifier_adapter.validate_strings(source_prop)}"""
            )
        if rel_props[0]["target_element_id_prop"] == target_prop:
            target_match_cypher = """toString(id(target))"""
        else:
            target_match_cypher = (
                f"""target.{gql_identifier_adapter.validate_strings(target_prop)}"""
            )
        cypher = f"""
        UNWIND $rel_list AS rel
        MATCH (source:{gql_identifier_adapter.validate_strings(source_label)})
        WHERE {source_match_cypher} = rel.source_prop
        MATCH (target:{gql_identifier_adapter.validate_strings(target_label)})
        WHERE {target_match_cypher} = rel.target_prop
        MERGE (source)-[r:{gql_identifier_adapter.validate_strings(rel_type)} {{ {merge_props} }}]->(target)
        ON MATCH SET r += rel.set_on_match
        ON CREATE SET r += rel.set_on_create
        SET r += rel.always_set
        RETURN r, source, target
        """

        params = {"rel_list": rel_props}

        from ..utils import get_node_types, get_rels_by_type

        rel_types = get_rels_by_type(rel_class)
        node_classes = get_node_types(rel_class.model_fields["source"].annotation)
        if (
            rel_class.model_fields["source"].annotation
            != rel_class.model_fields["target"].annotation
        ):
            node_classes.update(
                get_node_types(rel_class.model_fields["target"].annotation)
            )

        return self.evaluate_query(
            cypher, params, node_classes=node_classes, relationship_classes=rel_types
        )


class MemgraphConfig(GraphEngineConfig):
    engine: ClassVar[type[MemgraphEngine]] = MemgraphEngine
    uri: str
    username: str
    password: str

    @model_validator(mode="before")
    @classmethod
    def populate_defaults(cls, data: Any) -> Any:
        load_dotenv()

        if data.get("uri") is None:
            data["uri"] = os.getenv("MEMGRAPH_URI")

        if data.get("username") is None:
            data["username"] = os.getenv("MEMGRAPH_USERNAME")

        if data.get("password") is None:
            data["password"] = os.getenv("MEMGRAPH_PASSWORD")

        return data
