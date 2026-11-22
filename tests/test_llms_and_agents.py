"""The examples on the "LLMs and AI Agents" documentation page, run against the models it describes.

Kept in step with docs/llms-and-agents.md: if an example there changes, change it here.
"""

import json
from enum import Enum
from typing import ClassVar, Optional

import pytest
from pydantic import Field, ValidationError

from neontology import BaseNode, BaseRelationship, GraphConnection, get_ontology_schema
from neontology.graphengines.capabilities import Capability
from neontology.schema import cypher_patterns
from neontology.tools import import_records, record_model


class Sector(str, Enum):
    PUBLIC = "public"
    PRIVATE = "private"
    CHARITY = "charity"


class Entity(BaseNode):
    """Something the graph records facts about."""

    __primaryproperty__: ClassVar[str] = "slug"
    __primarylabel__: ClassVar[Optional[str]] = None  # abstract: shares its properties

    slug: str = Field(description="A unique, lower case identifier, such as 'ada-lovelace'.")
    name: str
    source_url: Optional[str] = Field(default=None, description="Where this was most recently learnt from.")


class Person(Entity):
    """A person."""

    __primarylabel__: ClassVar[Optional[str]] = "Person"


class Organisation(Entity):
    """A company, charity or public body."""

    __primarylabel__: ClassVar[Optional[str]] = "Organisation"

    sector: Optional[Sector] = None


class WorksFor(BaseRelationship):
    """Someone's employment at an organisation."""

    __relationshiptype__: ClassVar[str] = "WORKS_FOR"

    source: Person
    target: Organisation

    role: Optional[str] = Field(default=None, description="Their job title.")


MODELS = [Person, Organisation, WorksFor]

PATTERNS = """\
(:Person {slug: str PP, name: str, source_url: str})
  // A person.
  // slug: A unique, lower case identifier, such as 'ada-lovelace'.
  // source_url: Where this was most recently learnt from.
(:Organisation {slug: str PP, name: str, source_url: str, sector: "public"|"private"|"charity"})
  // A company, charity or public body.
  // slug: A unique, lower case identifier, such as 'ada-lovelace'.
  // source_url: Where this was most recently learnt from.

(:Person)-[:WORKS_FOR {role: str}]->(:Organisation)
  // Someone's employment at an organisation.
  // role: Their job title.
"""

ADA = {"LABEL": "Person", "slug": "ada-lovelace", "name": "Ada Lovelace"}
ENGINES = {"LABEL": "Organisation", "slug": "analytical-engines", "name": "Analytical Engines", "sector": "private"}
EMPLOYMENT = {
    "RELATIONSHIP_TYPE": "WORKS_FOR",
    "SOURCE_LABEL": "Person",
    "TARGET_LABEL": "Organisation",
    "SOURCE": "ada-lovelace",
    "TARGET": "analytical-engines",
    "role": "Analyst",
}


def _patterns():
    schema = get_ontology_schema(models=MODELS)

    return cypher_patterns(schema, capabilities=GraphConnection().engine.supported_capabilities)


def test_describing_the_graph_to_a_model(use_graph):
    assert _patterns().strip() == PATTERNS.strip()


def test_structured_output(use_graph):
    Records = record_model(models=MODELS)

    json_schema = Records.model_json_schema()

    assert set(json_schema["$defs"]) >= {"PersonRecord", "OrganisationRecord", "WorksForRecord"}

    # a strict mode writes null for what the model did not know
    response_text = json.dumps([{**ADA, "source_url": None}, ENGINES, EMPLOYMENT])

    records = Records.model_validate_json(response_text)

    import_records([record.model_dump(exclude_none=True) for record in records.root])

    assert WorksFor.get_count() == 1


def _extract(text, ask_model, patterns, attempts=3):
    """The page's extract(), with the patterns it reads passed in."""
    Records = record_model(models=MODELS)

    prompt = f"{patterns}\n\nRecord the people and organisations in this text:\n\n{text}"

    for _ in range(attempts):
        response_text = ask_model(prompt, Records.model_json_schema())

        try:
            # nulls left out: a strict structured-output mode writes null for what the model didn't know
            written = json.loads(response_text)
            content = [{key: value for key, value in record.items() if value is not None} for record in written]

            import_records(content, validate_only=True, error_on_unmatched=True)

        # invalid JSON, or content that didn't validate
        except ValueError as exc:
            prompt += f"\n\nYou wrote:\n{response_text}\n\nwhich has these problems:\n{exc}\n\nPlease correct them."
            continue

        return import_records(content)

    raise RuntimeError(f"No valid records after {attempts} attempts.")


def test_the_model_corrects_itself(use_graph):
    """A model's first answer misnames an end and gives a sector no one allows; its second is right."""
    answers = [
        json.dumps([ADA, {**ENGINES, "sector": "academic"}, EMPLOYMENT]),
        json.dumps([ADA, ENGINES, {**EMPLOYMENT, "TARGET": "analytical-engine"}]),
        json.dumps([ADA, ENGINES, EMPLOYMENT]),
    ]
    prompts = []

    def ask_model(prompt, json_schema):
        assert "WorksForRecord" in json_schema["$defs"]
        prompts.append(prompt)
        return answers[len(prompts) - 1]

    report = _extract("Ada Lovelace was an analyst at Analytical Engines.", ask_model, _patterns())

    assert len(prompts) == 3

    # each mistake reported once, against its record
    assert "1 problem in the content to import" in prompts[1]
    assert "record 2" in prompts[1] and "Input should be 'public', 'private' or 'charity'" in prompts[1]

    assert "No Organisation node has slug = 'analytical-engine'" in prompts[2] and "Please correct them" in prompts[2]

    assert report.nodes == {"Person": 1, "Organisation": 1}
    assert report.relationships == {"WORKS_FOR": 1}


def test_a_model_that_never_gets_it_right(use_graph):
    with pytest.raises(RuntimeError, match="No valid records after 2 attempts"):
        _extract("...", lambda prompt, schema: "not json", _patterns(), attempts=2)

    assert Person.get_count() == 0


def test_typed_queries_as_tools(use_graph):
    import_records([ADA, {**ENGINES, "source_url": "https://example.org/ae"}, EMPLOYMENT])

    assert [o.slug for o in Organisation.match_nodes(filters={"name__icontains": "engines"}, limit=20)] == [
        "analytical-engines"
    ]

    assert Person.get_count(filters={"source_url__isnull": True}) == 1

    ada = Person.match("ada-lovelace")
    assert ada is not None

    result = ada.get_related(relationship_types=["WORKS_FOR"], limit=20)

    assert [r.role for r in result.relationships if isinstance(r, WorksFor)] == ["Analyst"]

    # a misremembered property is something to correct, not an empty result
    with pytest.raises(ValueError, match="which is not one of its properties"):
        Organisation.match_nodes(filters={"title": "Analytical Engines"})


def test_queries_a_model_writes(use_graph):
    import_records([ADA, ENGINES, EMPLOYMENT])

    generated_cypher = "MATCH (p:Person)-[:WORKS_FOR]->(o:Organisation) RETURN o"

    gc = GraphConnection()

    if gc.engine.supports(Capability.READ_ONLY_QUERIES):
        result = gc.evaluate_query(generated_cypher, read_only=True)

        assert [o.slug for o in result.nodes if isinstance(o, Organisation)] == ["analytical-engines"]


def test_handing_results_back_and_amending_them(use_graph):
    import_records([ADA, ENGINES, EMPLOYMENT])

    ada = Person.match("ada-lovelace")
    assert ada is not None

    result = ada.get_related(relationship_types=["WORKS_FOR"], limit=20)

    dumped = json.loads(result.neontology_dump_json(nested=True))

    person = next(record for record in dumped if record["LABEL"] == "Person")

    assert person["RELATIONSHIPS_OUT"] == [
        {"role": "Analyst", "TARGET_LABEL": "Organisation", "RELATIONSHIP_TYPE": "WORKS_FOR", "TARGETS": ["analytical-engines"]}
    ]

    # what a model reads it can amend and give back as it is
    person["name"] = "Augusta Ada King"

    import_records(dumped)

    amended = Person.match("ada-lovelace")
    assert amended is not None and amended.name == "Augusta Ada King"


def test_looking_before_creating(use_graph):
    import_records([ENGINES])

    prompt = ""

    known = ", ".join(org.slug for org in Organisation.match_nodes(limit=500))

    prompt += f"\n\nOrganisations already recorded, by slug: {known}. Use these where they apply."

    assert "analytical-engines" in prompt


def test_provenance_set_by_the_program(use_graph):
    url = "https://example.org/ada"
    content = [dict(ADA), dict(ENGINES), dict(EMPLOYMENT)]

    for record in content:
        if "LABEL" in record:
            record["source_url"] = url

    import_records(content)

    ada = Person.match("ada-lovelace")
    assert ada is not None and ada.source_url == url


def test_an_enum_is_a_closed_vocabulary():
    with pytest.raises(ValidationError):
        record_model(models=MODELS).model_validate([{**ENGINES, "sector": "academic"}])
