from typing import ClassVar

import pytest

from neontology import BaseNode
from neontology.tools.import_files import import_json, import_md, import_yaml


class ExampleFileImportNode(BaseNode):
    __primarylabel__: ClassVar[str] = "ExampleFileImportLabel"
    __primaryproperty__: ClassVar[str] = "name"

    name: str
    description: str


def test_import_multiple_files(use_graph, tmp_path_factory):
    # write some markdowns to a tmpdir and use dir_path import
    dir_path = tmp_path_factory.mktemp("data")

    md1 = """
    ---
    LABEL: "ExampleFileImportLabel"
    BODY_PROPERTY: "description"
    name: "My first node"
    ---
    This is my first node!
    """

    md1_path = dir_path / "md1.md"

    with open(md1_path, "w") as md1_f:
        md1_f.write(md1)

    md2 = """
    ---
    LABEL: "ExampleFileImportLabel"
    BODY_PROPERTY: "description"
    name: "My second node"
    ---
    This is my second node!
    """

    md2_path = dir_path / "md2.md"

    with open(md2_path, "w") as md2_f:
        md2_f.write(md2)

    import_md(dir_path)

    cypher = """
    MATCH (n:ExampleFileImportLabel)
    RETURN COUNT(DISTINCT n)
    """

    result = use_graph.evaluate_query_single(cypher)

    assert result == 2


def test_import_single_file_md(use_graph, tmp_path_factory):
    # write some markdowns to a tmpdir and use dir_path import
    dir_path = tmp_path_factory.mktemp("data")

    md1 = """
    ---
    LABEL: "ExampleFileImportLabel"
    BODY_PROPERTY: "description"
    name: "My first node"
    ---
    This is my first node!
    """

    md1_path = dir_path / "md1.md"

    with open(md1_path, "w") as md1_f:
        md1_f.write(md1)

    import_md(md1_path)

    cypher = """
    MATCH (n:ExampleFileImportLabel)
    RETURN COUNT(DISTINCT n)
    """

    result = use_graph.evaluate_query_single(cypher)

    assert result == 1


def test_import_single_file_no_label(use_graph, tmp_path_factory):
    # write some markdowns to a tmpdir and use dir_path import
    dir_path = tmp_path_factory.mktemp("data")

    md1 = """
    ---
    BODY_PROPERTY: "description"
    name: "My first node"
    ---
    This is my first node!
    """

    md1_path = dir_path / "md1.md"

    with open(md1_path, "w") as md1_f:
        md1_f.write(md1)

    with pytest.raises(ValueError):
        import_md(md1_path)


def test_import_single_md_file_no_label(use_graph, tmp_path_factory):
    # write some markdowns to a tmpdir and use dir_path import
    dir_path = tmp_path_factory.mktemp("data")

    md1 = """
    ---
    LABEL: "ExampleFileImportLabel"
    name: "My first node"
    ---
    This is my first node!
    """

    md1_path = dir_path / "md1.md"

    with open(md1_path, "w") as md1_f:
        md1_f.write(md1)

    with pytest.raises(KeyError):
        import_md(md1_path)


def test_import_multiple_files_json(use_graph, tmp_path_factory):
    # write some markdowns to a tmpdir and use dir_path import
    dir_path = tmp_path_factory.mktemp("data")

    md1 = """{
        "LABEL": "ExampleFileImportLabel",
        "name": "My first node",
        "description": "This is my first node!"
    }"""

    md1_path = dir_path / "md1.json"

    with open(md1_path, "w") as md1_f:
        md1_f.write(md1)

    md2 = """{
            "LABEL": "ExampleFileImportLabel",
            "name": "My second node",
            "description": "This is my second node!"
        }"""

    md2_path = dir_path / "md2.json"

    with open(md2_path, "w") as md2_f:
        md2_f.write(md2)

    import_json(dir_path)

    cypher = """
    MATCH (n:ExampleFileImportLabel)
    RETURN COUNT(DISTINCT n)
    """

    result = use_graph.evaluate_query_single(cypher)

    assert result == 2


def test_import_single_file_json(use_graph, tmp_path_factory):
    # write some markdowns to a tmpdir and use dir_path import
    dir_path = tmp_path_factory.mktemp("data")

    md1 = """{
        "LABEL": "ExampleFileImportLabel",
        "name": "My first node",
        "description": "This is my first node!"
    }"""

    md1_path = dir_path / "md1.json"

    with open(md1_path, "w") as md1_f:
        md1_f.write(md1)

    import_json(md1_path)

    cypher = """
    MATCH (n:ExampleFileImportLabel)
    RETURN COUNT(DISTINCT n)
    """

    result = use_graph.evaluate_query_single(cypher)

    assert result == 1


def test_import_multiple_files_yaml(use_graph, tmp_path_factory):
    # write some markdowns to a tmpdir and use dir_path import
    dir_path = tmp_path_factory.mktemp("data")

    md1 = """
    LABEL: "ExampleFileImportLabel"
    name: "My first node"
    description: "This is my first node!"
    """

    md1_path = dir_path / "md1.yaml"

    with open(md1_path, "w") as md1_f:
        md1_f.write(md1)

    md2 = """
    LABEL: "ExampleFileImportLabel"
    name: "My second node"
    description: "This is my second node!"
    """

    md2_path = dir_path / "md2.yaml"

    with open(md2_path, "w") as md2_f:
        md2_f.write(md2)

    import_yaml(dir_path)

    cypher = """
    MATCH (n:ExampleFileImportLabel)
    RETURN COUNT(DISTINCT n)
    """

    result = use_graph.evaluate_query_single(cypher)

    assert result == 2


def test_import_single_file_yaml(use_graph, tmp_path_factory):
    # write some markdowns to a tmpdir and use dir_path import
    dir_path = tmp_path_factory.mktemp("data")

    md1 = """---
LABEL: "ExampleFileImportLabel"
name: "My first node"
description: "This is my first node!"
---
LABEL: "ExampleFileImportLabel"
name: "My second node"
description: "This is my second node!"
"""

    md1_path = dir_path / "md1.yaml"

    with open(md1_path, "w") as md1_f:
        md1_f.write(md1)

    import_yaml(md1_path)

    cypher = """
    MATCH (n:ExampleFileImportLabel)
    RETURN COUNT(DISTINCT n)
    """

    result = use_graph.evaluate_query_single(cypher)

    assert result == 2
