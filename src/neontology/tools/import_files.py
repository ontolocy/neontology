import json
from logging import getLogger
from pathlib import Path
from typing import Generator, List, Optional, Sequence

import yaml

from .import_records import import_records

logger = getLogger(__name__)


def _identify_filepaths(input_path: str, path_pattern: str) -> List[Path]:
    path = Path(input_path)

    if path.is_file():
        return [path]

    return list(path.glob(path_pattern))


def _batch(iterable: Sequence, batch_size: int = 1) -> Generator:
    full_length = len(iterable)
    for ndx in range(0, full_length, batch_size):
        yield iterable[ndx : min(ndx + batch_size, full_length)]  # noqa: E203


def import_json(
    path: str,
    path_pattern: str = "**/*.json",
    batch_size: Optional[int] = None,
    check_unmatched: bool = True,
    error_on_unmatched: bool = False,
    validate_only: bool = False,
) -> None:
    file_paths = _identify_filepaths(path, path_pattern)

    if len(file_paths) == 0:
        logger.warning("Didn't find any files to import")
        return

    if not batch_size:
        batch_size = len(file_paths)

    for file_batch in _batch(file_paths, batch_size):
        input_records = []

        for file_path in file_batch:
            logger.info("Processing %s", file_path)
            with open(file_path, "r") as json_file:
                raw_record_data = json.load(json_file)

            input_records.append(raw_record_data)

        import_records(
            input_records,
            check_unmatched=check_unmatched,
            error_on_unmatched=error_on_unmatched,
            validate_only=validate_only,
        )


def import_yaml(
    path: str,
    path_pattern: str = "**/*.yaml",
    batch_size: Optional[int] = None,
    check_unmatched: bool = True,
    error_on_unmatched: bool = False,
    validate_only: bool = False,
) -> None:
    file_paths = _identify_filepaths(path, path_pattern)

    if len(file_paths) == 0:
        logger.warning("Didn't find any files to import")
        return

    if batch_size is None:
        batch_size = len(file_paths)

    for file_batch in _batch(file_paths, batch_size):
        input_records = []

        for file_path in file_batch:
            logger.info("Processing %s", file_path)
            with open(file_path, "r") as yaml_file:
                raw_record_data = yaml.safe_load_all(yaml_file)

                input_records.append([x for x in raw_record_data])

        import_records(
            input_records,
            check_unmatched=check_unmatched,
            error_on_unmatched=error_on_unmatched,
            validate_only=validate_only,
        )


def import_md(
    path: str,
    path_pattern: str = "**/*.md",
    batch_size: Optional[int] = None,
    check_unmatched: bool = True,
    error_on_unmatched: bool = False,
    validate_only: bool = False,
) -> None:
    file_paths = _identify_filepaths(path, path_pattern)

    if len(file_paths) == 0:
        logger.warning("Didn't find any files to import")
        return

    if not batch_size:
        batch_size = len(file_paths)

    for file_batch in _batch(file_paths, batch_size):
        input_records = []

        for file_path in file_batch:
            logger.info("Processing %s", file_path)

            with open(file_path, "r") as md_file:
                raw_entry = md_file.read()

            entry = raw_entry.strip().split("---", maxsplit=2)

            frontmatter = entry[1]
            markdown_text = entry[2].strip()

            record = next(yaml.safe_load_all(frontmatter))
            body_property = record.pop("BODY_PROPERTY")

            record[body_property] = markdown_text

            input_records.append(record)

        import_records(
            input_records,
            check_unmatched=check_unmatched,
            error_on_unmatched=error_on_unmatched,
            validate_only=validate_only,
        )
