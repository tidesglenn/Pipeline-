from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, Iterable, List, Tuple


def validate_common_row(row: Dict[str, str]) -> Tuple[bool, str]:
    source_path = row.get("SourcePath", "").strip()
    if not source_path:
        return False, "Source path is required"
    if not os.path.exists(source_path):
        return False, "Source path does not exist"
    return True, "Ready"


def validate_unzip_row(row: Dict[str, str]) -> Tuple[bool, str]:
    ok, msg = validate_common_row(row)
    if not ok:
        return ok, msg
    tool_path = row.get("ToolPath", "").strip()
    if not tool_path or not os.path.exists(tool_path):
        return False, "7-Zip path is missing or invalid"
    dest = row.get("FinalDestination", "").strip()
    if not dest:
        return False, "Final destination is required"
    return True, "Ready"


def validate_copy_row(row: Dict[str, str]) -> Tuple[bool, str]:
    ok, msg = validate_common_row(row)
    if not ok:
        return ok, msg
    dest = row.get("CopyDestination", "").strip() or row.get("FinalDestination", "").strip()
    if not dest:
        return False, "Destination is required"
    return True, "Ready"


def detect_duplicate_destinations(rows: Iterable[Dict[str, str]], key_name: str) -> List[str]:
    seen = {}
    duplicates: List[str] = []
    for row in rows:
        value = row.get(key_name, "").strip().lower()
        row_id = row.get("RowID", "")
        if not value:
            continue
        if value in seen:
            duplicates.append(row_id)
            duplicates.append(seen[value])
        else:
            seen[value] = row_id
    return sorted(set(duplicates))


def ensure_parent_folder(path: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
