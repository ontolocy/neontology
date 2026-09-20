"""Importing flat CSV rows."""

from typing import ClassVar, Optional

import pytest

from neontology import BaseNode, BaseRelationship
from neontology.tools import ImportContentError, import_csv


class CsvHost(BaseNode):
    __primarylabel__: ClassVar[str] = "CsvHost"
    __primaryproperty__: ClassVar[str] = "hostname"

    hostname: str
    owner: Optional[str] = None
    cores: Optional[int] = None
    retired: bool = False


class CsvSite(BaseNode):
    __primarylabel__: ClassVar[str] = "CsvSite"
    __primaryproperty__: ClassVar[str] = "code"

    code: str


class CsvLocatedIn(BaseRelationship):
    __relationshiptype__: ClassVar[str] = "CSV_LOCATED_IN"

    source: CsvHost
    target: CsvSite

    since: Optional[int] = None


def _write(dir_path, name, text):
    (dir_path / name).write_text(text)

    return dir_path / name


def test_import_nodes_with_a_label_column(use_graph, tmp_path_factory):
    dir_path = tmp_path_factory.mktemp("csv")

    _write(
        dir_path,
        "hosts.csv",
        "LABEL,hostname,owner\nCsvHost,web1,platform\nCsvHost,web2,data\n",
    )

    report = import_csv(dir_path)

    assert report.nodes == {"CsvHost": 2}
    assert CsvHost.match("web1").owner == "platform"


def test_import_nodes_with_the_label_given_once(use_graph, tmp_path_factory):
    # the label is the same for every row, so it does not need a column of its own
    dir_path = tmp_path_factory.mktemp("csv")

    _write(dir_path, "hosts.csv", "hostname,owner\nweb1,platform\nweb2,data\n")

    report = import_csv(dir_path, defaults={"LABEL": "CsvHost"})

    assert report.nodes == {"CsvHost": 2}


def test_a_column_overrides_the_default(use_graph, tmp_path_factory):
    dir_path = tmp_path_factory.mktemp("csv")

    _write(dir_path, "mixed.csv", "LABEL,hostname,code\nCsvHost,web1,\nCsvSite,,LON\n")

    report = import_csv(dir_path, defaults={"LABEL": "CsvHost"})

    assert report.nodes == {"CsvHost": 1, "CsvSite": 1}


def test_values_are_converted_by_the_model(use_graph, tmp_path_factory):
    dir_path = tmp_path_factory.mktemp("csv")

    _write(dir_path, "hosts.csv", "hostname,cores,retired\nweb1,16,true\n")

    import_csv(dir_path, defaults={"LABEL": "CsvHost"})

    host = CsvHost.match("web1")

    assert host.cores == 16
    assert host.retired is True


def test_an_empty_cell_means_the_property_was_not_given(use_graph, tmp_path_factory):
    # so the model's default applies, rather than the property being set to empty
    dir_path = tmp_path_factory.mktemp("csv")

    _write(dir_path, "hosts.csv", "hostname,owner,cores,retired\nweb1,,,\n")

    import_csv(dir_path, defaults={"LABEL": "CsvHost"})

    host = CsvHost.match("web1")

    assert host.owner is None
    assert host.cores is None
    assert host.retired is False


def test_import_relationships(use_graph, tmp_path_factory):
    dir_path = tmp_path_factory.mktemp("csv")

    _write(dir_path, "a_hosts.csv", "LABEL,hostname\nCsvHost,web1\nCsvHost,web2\n")
    _write(dir_path, "b_sites.csv", "LABEL,code\nCsvSite,LON\n")
    _write(
        dir_path,
        "c_rels.csv",
        "RELATIONSHIP_TYPE,SOURCE_LABEL,TARGET_LABEL,SOURCE,TARGET,since\n"
        "CSV_LOCATED_IN,CsvHost,CsvSite,web1,LON,2019\n"
        "CSV_LOCATED_IN,CsvHost,CsvSite,web2,LON,2021\n",
    )

    report = import_csv(dir_path, error_on_unmatched=True)

    assert report.relationships == {"CSV_LOCATED_IN": 2}
    assert {x.since for x in CsvLocatedIn.match_relationships()} == {2019, 2021}


def test_relationship_defaults_keep_the_columns_to_the_data(use_graph, tmp_path_factory):
    # defaults describe the rows one call reads, so files of different kinds are
    # imported by a call each
    dir_path = tmp_path_factory.mktemp("csv")

    _write(dir_path, "nodes_hosts.csv", "LABEL,hostname\nCsvHost,web1\n")
    _write(dir_path, "nodes_sites.csv", "LABEL,code\nCsvSite,LON\n")
    _write(dir_path, "rels.csv", "SOURCE,TARGET\nweb1,LON\n")

    import_csv(dir_path, path_pattern="nodes_*.csv")

    report = import_csv(
        dir_path / "rels.csv",
        defaults={
            "RELATIONSHIP_TYPE": "CSV_LOCATED_IN",
            "SOURCE_LABEL": "CsvHost",
            "TARGET_LABEL": "CsvSite",
        },
        error_on_unmatched=True,
    )

    assert report.relationships == {"CSV_LOCATED_IN": 1}


def test_a_relationship_key_on_a_node_record_is_reported(use_graph, tmp_path_factory):
    dir_path = tmp_path_factory.mktemp("csv")

    _write(dir_path, "hosts.csv", "LABEL,hostname\nCsvHost,web1\n")

    with pytest.raises(ImportContentError, match="relationship record"):
        import_csv(dir_path, defaults={"RELATIONSHIP_TYPE": "CSV_LOCATED_IN"})


def test_a_row_which_does_not_validate_names_its_line(use_graph, tmp_path_factory):
    dir_path = tmp_path_factory.mktemp("csv")

    _write(
        dir_path,
        "hosts.csv",
        "hostname,cores\nweb1,16\nweb2,32\nbroken,not-a-number\n",
    )

    with pytest.raises(ImportContentError) as raised:
        import_csv(dir_path, defaults={"LABEL": "CsvHost"})

    message = str(raised.value)

    assert "hosts.csv" in message
    # the header is line 1, so the bad row is line 4 of the file
    assert "line 4" in message


def test_nesting_is_not_supported_and_says_so(use_graph, tmp_path_factory):
    dir_path = tmp_path_factory.mktemp("csv")

    _write(dir_path, "hosts.csv", "LABEL,hostname,RELATIONSHIPS_OUT\nCsvHost,web1,something\n")

    with pytest.raises(ImportContentError, match="RELATIONSHIPS_OUT"):
        import_csv(dir_path)


def test_inline_node_definition_is_not_supported_and_says_so(use_graph, tmp_path_factory):
    dir_path = tmp_path_factory.mktemp("csv")

    _write(
        dir_path,
        "rels.csv",
        "RELATIONSHIP_TYPE,SOURCE_LABEL,TARGET_LABEL,SOURCE,TARGET_NODES\nCSV_LOCATED_IN,CsvHost,CsvSite,web1,x\n",
    )

    with pytest.raises(ImportContentError, match="TARGET_NODES"):
        import_csv(dir_path)


def test_a_file_with_no_header_is_reported(use_graph, tmp_path_factory):
    dir_path = tmp_path_factory.mktemp("csv")

    _write(dir_path, "empty.csv", "")

    with pytest.raises(ImportContentError, match="empty.csv"):
        import_csv(dir_path)


def test_validate_only_works_for_csv(use_graph, tmp_path_factory):
    dir_path = tmp_path_factory.mktemp("csv")

    _write(dir_path, "hosts.csv", "hostname,cores\nweb1,16\n")

    report = import_csv(dir_path, defaults={"LABEL": "CsvHost"}, validate_only=True)

    assert report.validated_only is True
    assert CsvHost.get_count() == 0
