import json
from logging import getLogger
from pathlib import Path
from typing import Optional, Union

import yaml

from .errors import ImportContentError
from .import_records import import_sourced_records
from .origin import RecordOrigin, SourcedRecord
from .records import iter_raw_records
from .report import ImportReport

logger = getLogger(__name__)


def _identify_filepaths(input_path: Union[str, Path], path_pattern: str) -> list[Path]:
    """Find the files to import, in a stable order.

    Sorted, because the glob's own order comes from the filesystem: the same content
    would otherwise be read in a different order on a different machine.

    Args:
        input_path (Union[str, Path]): a file, or a directory to search.
        path_pattern (str): the glob pattern to match files in a directory.

    Returns:
        list[Path]: the paths to import, sorted.
    """
    path = Path(input_path)

    if path.is_file():
        return [path]

    return sorted(path.glob(path_pattern))


def _read_json(file_path: Path) -> list[SourcedRecord]:
    """Read the records in a JSON file.

    Args:
        file_path (Path): the file to read.

    Returns:
        list[SourcedRecord]: the records it holds, each knowing where it came from.

    Raises:
        ImportContentError: if the file is not valid JSON.
    """
    with open(file_path, "r") as json_file:
        try:
            loaded = json.load(json_file)

        except json.JSONDecodeError as exc:
            raise ImportContentError(f"{file_path} is not valid JSON: {exc}") from exc

    return iter_raw_records(loaded, RecordOrigin(source=str(file_path)))


def _read_yaml(file_path: Path) -> list[SourcedRecord]:
    """Read the records in a YAML file.

    A file may hold one record, a list of records, or several documents of either.

    Args:
        file_path (Path): the file to read.

    Returns:
        list[SourcedRecord]: the records it holds, each knowing where it came from.

    Raises:
        ImportContentError: if the file is not valid YAML.
    """
    with open(file_path, "r") as yaml_file:
        try:
            documents = list(yaml.safe_load_all(yaml_file))

        except yaml.YAMLError as exc:
            raise ImportContentError(f"{file_path} is not valid YAML: {exc}") from exc

    origin = RecordOrigin(source=str(file_path))

    records: list[SourcedRecord] = []

    # a document is numbered only where the file holds more than one, so a single
    # document file reports a record's position without a document to qualify it
    multiple = len(documents) > 1

    for position, document in enumerate(documents):
        if document is None:
            continue

        document_origin = origin.in_document(position) if multiple else origin

        records += iter_raw_records(document, document_origin)

    return records


def _read_md(file_path: Path) -> list[SourcedRecord]:
    """Read the record in a markdown file with YAML frontmatter.

    Args:
        file_path (Path): the file to read.

    Returns:
        list[SourcedRecord]: the single record it holds.

    Raises:
        ImportContentError: if the file has no frontmatter, its frontmatter is not
            valid YAML, or it does not say which property the body belongs in.
    """
    with open(file_path, "r") as md_file:
        raw_entry = md_file.read()

    entry = raw_entry.strip().split("---", maxsplit=2)

    if len(entry) < 3:
        raise ImportContentError(
            f"{file_path} has no frontmatter. A markdown record starts with a block fenced by '---'"
            " lines, holding the record's LABEL and properties as YAML."
        )

    frontmatter = entry[1]
    markdown_text = entry[2].strip()

    try:
        record = next(yaml.safe_load_all(frontmatter))

    except yaml.YAMLError as exc:
        raise ImportContentError(f"The frontmatter in {file_path} is not valid YAML: {exc}") from exc

    if "BODY_PROPERTY" not in record:
        raise ImportContentError(
            f"{file_path} does not set BODY_PROPERTY, so there is nowhere to put the markdown body."
            " Name the property the text belongs in, for example BODY_PROPERTY: description."
        )

    body_property = record.pop("BODY_PROPERTY")

    record[body_property] = markdown_text

    return [SourcedRecord(data=record, origin=RecordOrigin(source=str(file_path)))]


def _import_files(
    path: Union[str, Path],
    path_pattern: str,
    reader,
    batch_size: Optional[int],
    check_unmatched: bool,
    error_on_unmatched: bool,
    validate_only: bool,
) -> ImportReport:
    """Read every matching file and import what they hold as one set of records.

    Every file is read before anything is written, so a relationship in one file can
    refer to a node defined in any other - including one read after it.

    Args:
        path (Union[str, Path]): a file, or a directory to search.
        path_pattern (str): the glob pattern to match files in a directory.
        reader: reads one file, returning the records it holds.
        batch_size (Optional[int]): how many records to write per query.
        check_unmatched (bool): check relationship endpoints resolve.
        error_on_unmatched (bool): raise rather than warn where they do not.
        validate_only (bool): do not populate the graph.

    Returns:
        ImportReport: what the import did, and which files it read.
    """
    file_paths = _identify_filepaths(path, path_pattern)

    report = ImportReport(files=[str(x) for x in file_paths], validated_only=validate_only)

    if len(file_paths) == 0:
        logger.warning("Didn't find any files to import")
        return report

    input_records: list[SourcedRecord] = []

    for file_path in file_paths:
        logger.info("Processing %s", file_path)

        input_records += reader(file_path)

    return import_sourced_records(
        input_records,
        check_unmatched=check_unmatched,
        error_on_unmatched=error_on_unmatched,
        validate_only=validate_only,
        batch_size=batch_size,
        report=report,
    )


def import_json(
    path: str,
    path_pattern: str = "**/*.json",
    batch_size: Optional[int] = None,
    check_unmatched: bool = True,
    error_on_unmatched: bool = False,
    validate_only: bool = False,
) -> ImportReport:
    """Import JSON files into the graph.

    Args:
        path (str): a file, or a directory to search for files.
        path_pattern (str): the glob pattern to match files in a directory.
        batch_size (Optional[int]): how many records to write per query. It limits the
            size of each write, not what a relationship can refer to.
        check_unmatched (bool): check relationship endpoints resolve, warn if not.
        error_on_unmatched (bool): raise rather than warn where they do not.
        validate_only (bool): do not populate the graph.

    Returns:
        ImportReport: what the import did, and which files it read.
    """
    return _import_files(
        path,
        path_pattern,
        _read_json,
        batch_size,
        check_unmatched,
        error_on_unmatched,
        validate_only,
    )


def import_yaml(
    path: str,
    path_pattern: str = "**/*.yaml",
    batch_size: Optional[int] = None,
    check_unmatched: bool = True,
    error_on_unmatched: bool = False,
    validate_only: bool = False,
) -> ImportReport:
    """Import YAML files into the graph.

    Each file holds a record, a list of records, or several documents of either, where
    a node record has a LABEL and a relationship record a RELATIONSHIP_TYPE.

    Args:
        path (str): a file, or a directory to search for files.
        path_pattern (str): the glob pattern to match files in a directory.
        batch_size (Optional[int]): how many records to write per query. It limits the
            size of each write, not what a relationship can refer to.
        check_unmatched (bool): check relationship endpoints resolve, warn if not.
        error_on_unmatched (bool): raise rather than warn where they do not.
        validate_only (bool): do not populate the graph.

    Returns:
        ImportReport: what the import did, and which files it read.
    """
    return _import_files(
        path,
        path_pattern,
        _read_yaml,
        batch_size,
        check_unmatched,
        error_on_unmatched,
        validate_only,
    )


def import_md(
    path: str,
    path_pattern: str = "**/*.md",
    batch_size: Optional[int] = None,
    check_unmatched: bool = True,
    error_on_unmatched: bool = False,
    validate_only: bool = False,
) -> ImportReport:
    """Import markdown files with frontmatter into the graph.

    The frontmatter should be in YAML format and contain LABEL and a BODY_PROPERTY key
    naming the property the markdown body belongs in.

    Args:
        path (str): a file, or a directory to search for files.
        path_pattern (str): the glob pattern to match files in a directory.
        batch_size (Optional[int]): how many records to write per query. It limits the
            size of each write, not what a relationship can refer to.
        check_unmatched (bool): check relationship endpoints resolve, warn if not.
        error_on_unmatched (bool): raise rather than warn where they do not.
        validate_only (bool): do not populate the graph.

    Returns:
        ImportReport: what the import did, and which files it read.
    """
    return _import_files(
        path,
        path_pattern,
        _read_md,
        batch_size,
        check_unmatched,
        error_on_unmatched,
        validate_only,
    )
