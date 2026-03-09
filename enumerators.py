from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Dict, List

from .models import make_blank_row

ARCHIVE_EXTENSIONS = {".zip", ".7z", ".rar", ".tar", ".gz", ".bz2", ".xz"}


def _next_row_id(prefix: str, rows: List[Dict[str, str]]) -> str:
    max_number = 0
    for row in rows:
        row_id = row.get("RowID", "")
        if row_id.startswith(prefix):
            try:
                max_number = max(max_number, int(row_id[len(prefix):]))
            except ValueError:
                pass
    return f"{prefix}{max_number + 1:04d}"


def enumerate_unzip_jobs(
    source_path: str,
    destination_root: str,
    existing_rows: List[Dict[str, str]],
    seven_zip_path: str,
    include_subfolders: bool,
    overwrite_existing: bool,
    test_before_extract: bool,
) -> List[Dict[str, str]]:
    source = Path(source_path)
    rows: List[Dict[str, str]] = []
    files: List[Path] = []
    if source.is_file() and source.suffix.lower() in ARCHIVE_EXTENSIONS:
        files = [source]
    elif source.is_dir():
        pattern = "**/*" if include_subfolders else "*"
        for entry in source.glob(pattern):
            if entry.is_file() and entry.suffix.lower() in ARCHIVE_EXTENSIONS:
                files.append(entry)
    for archive in files:
        row_id = _next_row_id("UZ", existing_rows + rows)
        row = make_blank_row(row_id, "Unzip")
        stem = archive.name
        for ext in archive.suffixes:
            stem = stem[: -len(ext)] if stem.endswith(ext) else stem
        final_dest = str(Path(destination_root) / stem)
        options = {
            "include_subfolders": include_subfolders,
            "overwrite_existing": overwrite_existing,
            "test_before_extract": test_before_extract,
        }
        row.update(
            {
                "JobName": archive.name,
                "SourcePath": str(archive),
                "FinalDestination": final_dest,
                "ToolPath": seven_zip_path,
                "Status": "Draft",
                "OptionsJson": json.dumps(options),
            }
        )
        rows.append(row)
    return rows


def enumerate_copy_jobs(
    source_path: str,
    destination_root: str,
    existing_rows: List[Dict[str, str]],
    include_subfolders: bool,
    by_immediate_child: bool,
) -> List[Dict[str, str]]:
    source = Path(source_path)
    rows: List[Dict[str, str]] = []
    items: List[Path] = []
    if source.is_file():
        items = [source]
    elif source.is_dir():
        if by_immediate_child:
            items = [item for item in source.iterdir()]
        else:
            items = [source]
    for item in items:
        row_id = _next_row_id("CP", existing_rows + rows)
        row = make_blank_row(row_id, "Copy")
        target = str(Path(destination_root) / item.name)
        options = {
            "include_subfolders": include_subfolders,
            "by_immediate_child": by_immediate_child,
        }
        row.update(
            {
                "JobName": item.name,
                "SourcePath": str(item),
                "CopyDestination": target,
                "FinalDestination": target,
                "ToolPath": "robocopy",
                "Status": "Draft",
                "OptionsJson": json.dumps(options),
            }
        )
        rows.append(row)
    return rows
