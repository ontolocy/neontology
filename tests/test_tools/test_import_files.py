from typing import ClassVar, Optional

import pytest

from neontology import BaseNode, BaseRelationship
from neontology.tools.errors import ImportContentError
from neontology.tools.import_files import (
    _identify_filepaths,
    import_json,
    import_md,
    import_yaml,
)


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

    assert ExampleFileImportNode.get_count() == 2


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

    assert ExampleFileImportNode.get_count() == 1


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


def test_import_single_md_file_no_body_property(use_graph, tmp_path_factory):
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

    # used to be a bare KeyError, which said nothing about which file was at fault
    with pytest.raises(ImportContentError):
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

    assert ExampleFileImportNode.get_count() == 2


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

    assert ExampleFileImportNode.get_count() == 1


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

    assert ExampleFileImportNode.get_count() == 2


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

    assert ExampleFileImportNode.get_count() == 2


class SplitImportNode(BaseNode):
    __primarylabel__: ClassVar[str] = "SplitImportLabel"
    __primaryproperty__: ClassVar[str] = "name"

    name: str
    description: Optional[str] = None


class SplitImportRel(BaseRelationship):
    __relationshiptype__: ClassVar[str] = "SPLIT_IMPORT_LINKS_TO"

    source: SplitImportNode
    target: SplitImportNode


def test_import_yaml_top_level_list(use_graph, tmp_path_factory):
    # a yaml file whose document is a list of records - the natural way to write
    # several entries in one file
    dir_path = tmp_path_factory.mktemp("data")

    (dir_path / "nodes.yaml").write_text(
        '- LABEL: "ExampleFileImportLabel"\n'
        '  name: "My first node"\n'
        '  description: "This is my first node!"\n'
        '- LABEL: "ExampleFileImportLabel"\n'
        '  name: "My second node"\n'
        '  description: "This is my second node!"\n'
    )

    import_yaml(dir_path)

    assert ExampleFileImportNode.get_count() == 2


def test_import_yaml_multi_document_still_works(use_graph, tmp_path_factory):
    dir_path = tmp_path_factory.mktemp("data")

    (dir_path / "nodes.yaml").write_text(
        '---\nLABEL: "ExampleFileImportLabel"\nname: "n1"\ndescription: "d1"\n'
        '---\nLABEL: "ExampleFileImportLabel"\nname: "n2"\ndescription: "d2"\n'
    )

    import_yaml(dir_path)

    assert ExampleFileImportNode.get_count() == 2


def test_split_node_and_relationship_files(use_graph, tmp_path_factory):
    # the relationship file sorts before the node file, so relationships can only
    # resolve if every node is written before any relationship is
    dir_path = tmp_path_factory.mktemp("data")

    (dir_path / "a_relationships.yaml").write_text(
        "- RELATIONSHIP_TYPE: SPLIT_IMPORT_LINKS_TO\n"
        "  SOURCE_LABEL: SplitImportLabel\n"
        "  TARGET_LABEL: SplitImportLabel\n"
        "  SOURCE: alpha\n"
        "  TARGET: beta\n"
    )

    (dir_path / "z_nodes.yaml").write_text(
        "- LABEL: SplitImportLabel\n  name: alpha\n- LABEL: SplitImportLabel\n  name: beta\n"
    )

    import_yaml(dir_path, error_on_unmatched=True)

    assert SplitImportNode.get_count() == 2
    assert len(SplitImportRel.match_relationships()) == 1


def test_split_files_resolve_across_batches(use_graph, tmp_path_factory):
    # batch_size batches the writes, it does not limit what a relationship can refer to
    dir_path = tmp_path_factory.mktemp("data")

    (dir_path / "a_relationships.yaml").write_text(
        "- RELATIONSHIP_TYPE: SPLIT_IMPORT_LINKS_TO\n"
        "  SOURCE_LABEL: SplitImportLabel\n"
        "  TARGET_LABEL: SplitImportLabel\n"
        "  SOURCE: alpha\n"
        "  TARGET: beta\n"
    )

    (dir_path / "z_nodes.yaml").write_text(
        "- LABEL: SplitImportLabel\n  name: alpha\n- LABEL: SplitImportLabel\n  name: beta\n"
    )

    import_yaml(dir_path, batch_size=1, error_on_unmatched=True)

    assert len(SplitImportRel.match_relationships()) == 1


def test_file_paths_are_discovered_in_a_stable_order(tmp_path_factory):
    dir_path = tmp_path_factory.mktemp("data")

    for name in ["c.yaml", "a.yaml", "b.yaml"]:
        (dir_path / name).write_text("LABEL: SplitImportLabel\nname: x\n")

    found = _identify_filepaths(str(dir_path), "**/*.yaml")

    assert [x.name for x in found] == ["a.yaml", "b.yaml", "c.yaml"]


def test_import_md_without_frontmatter_names_the_file(use_graph, tmp_path_factory):
    dir_path = tmp_path_factory.mktemp("data")

    md_path = dir_path / "nofrontmatter.md"
    md_path.write_text("Just some prose, with no frontmatter at all.\n")

    with pytest.raises(ImportContentError, match="nofrontmatter.md"):
        import_md(md_path)


def test_import_md_without_body_property_names_the_file(use_graph, tmp_path_factory):
    dir_path = tmp_path_factory.mktemp("data")

    md_path = dir_path / "nobodyprop.md"
    md_path.write_text('---\nLABEL: "ExampleFileImportLabel"\nname: "x"\n---\nbody\n')

    with pytest.raises(ImportContentError, match="nobodyprop.md"):
        import_md(md_path)


def test_import_yaml_with_invalid_yaml_names_the_file(use_graph, tmp_path_factory):
    dir_path = tmp_path_factory.mktemp("data")

    (dir_path / "broken.yaml").write_text("LABEL: ExampleFileImportLabel\n  bad: [indentation\n")

    with pytest.raises(ImportContentError, match="broken.yaml"):
        import_yaml(dir_path)


def test_import_json_with_invalid_json_names_the_file(use_graph, tmp_path_factory):
    dir_path = tmp_path_factory.mktemp("data")

    (dir_path / "broken.json").write_text('{"LABEL": "ExampleFileImportLabel",,}')

    with pytest.raises(ImportContentError, match="broken.json"):
        import_json(dir_path)
