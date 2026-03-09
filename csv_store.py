from __future__ import annotations

import csv
import os
import tempfile
from typing import Dict, Iterable, List, Optional

from .models import QUEUE_COLUMNS


def ensure_csv(path: str) -> None:
    if os.path.exists(path):
        return
    folder = os.path.dirname(path)
    if folder:
        os.makedirs(folder, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=QUEUE_COLUMNS)
        writer.writeheader()


def load_queue_csv(path: str) -> List[Dict[str, str]]:
    ensure_csv(path)
    rows: List[Dict[str, str]] = []
    with open(path, "r", newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        for raw in reader:
            row = {key: raw.get(key, "") for key in QUEUE_COLUMNS}
            rows.append(row)
    return rows


def save_queue_csv(path: str, rows: Iterable[Dict[str, str]]) -> None:
    folder = os.path.dirname(path)
    if folder:
        os.makedirs(folder, exist_ok=True)
    fd, temp_path = tempfile.mkstemp(prefix="queue_", suffix=".csv", dir=folder or None)
    os.close(fd)
    try:
        with open(temp_path, "w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=QUEUE_COLUMNS)
            writer.writeheader()
            for row in rows:
                safe = {key: row.get(key, "") for key in QUEUE_COLUMNS}
                writer.writerow(safe)
        os.replace(temp_path, path)
    finally:
        if os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except OSError:
                pass


def append_rows(path: str, new_rows: Iterable[Dict[str, str]]) -> None:
    rows = load_queue_csv(path)
    rows.extend(new_rows)
    save_queue_csv(path, rows)


def update_row(path: str, row_id: str, updates: Dict[str, str]) -> Optional[Dict[str, str]]:
    rows = load_queue_csv(path)
    changed: Optional[Dict[str, str]] = None
    for row in rows:
        if row.get("RowID", "") == row_id:
            row.update({key: str(value) for key, value in updates.items()})
            changed = row
            break
    if changed is not None:
        save_queue_csv(path, rows)
    return changed
