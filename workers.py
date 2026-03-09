from __future__ import annotations

import os
import time
from datetime import datetime
from typing import Dict

from .csv_store import update_row
from .progress import scan_tree_stats
from .tools import run_7zip, run_python_copy


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _mark_running(queue_csv: str, row: Dict[str, str], phase: str, log_path: str) -> None:
    update_row(
        queue_csv,
        row["RowID"],
        {
            "Status": "Running",
            "Phase": phase,
            "StartTime": _now_text(),
            "LastUpdateTime": _now_text(),
            "LogPath": log_path,
            "LastMessage": f"{phase} started",
        },
    )


def _mark_done(queue_csv: str, row: Dict[str, str], phase: str, output_path: str) -> None:
    file_count, folder_count, output_bytes = scan_tree_stats(output_path)
    update_row(
        queue_csv,
        row["RowID"],
        {
            "Status": "Completed",
            "Phase": phase,
            "PercentComplete": "100",
            "LastUpdateTime": _now_text(),
            "EndTime": _now_text(),
            "OutputBytes": str(output_bytes),
            "FileCount": str(file_count),
            "FolderCount": str(folder_count),
            "LastMessage": f"{phase} completed",
        },
    )


def _mark_failed(queue_csv: str, row: Dict[str, str], phase: str, code: str, message: str) -> None:
    update_row(
        queue_csv,
        row["RowID"],
        {
            "Status": "Failed",
            "Phase": phase,
            "LastUpdateTime": _now_text(),
            "EndTime": _now_text(),
            "ErrorPhase": phase,
            "ErrorCode": code,
            "ErrorMessage": message,
            "LastMessage": message,
        },
    )


def process_unzip_row(queue_csv: str, row: Dict[str, str], log_dir: str) -> None:
    log_path = os.path.join(log_dir, f"{row['RowID']}.log")
    _mark_running(queue_csv, row, "Unzipping", log_path)
    start_ts = time.time()
    result = run_7zip(row, log_path)
    elapsed = int(time.time() - start_ts)
    if result.returncode == 0:
        _mark_done(queue_csv, row, "Unzipping", row.get("FinalDestination", ""))
        update_row(queue_csv, row["RowID"], {"ElapsedSeconds": str(elapsed), "ExitCode": str(result.returncode)})
    else:
        _mark_failed(queue_csv, row, "Unzipping", f"7ZIP_{result.returncode}", "Archive extraction failed")
        update_row(queue_csv, row["RowID"], {"ElapsedSeconds": str(elapsed), "ExitCode": str(result.returncode)})


def process_copy_row(queue_csv: str, row: Dict[str, str], log_dir: str) -> None:
    log_path = os.path.join(log_dir, f"{row['RowID']}.log")
    _mark_running(queue_csv, row, "Copying", log_path)
    start_ts = time.time()
    rc = run_python_copy(row, log_path)
    elapsed = int(time.time() - start_ts)
    output_path = row.get("CopyDestination", "") or row.get("FinalDestination", "")
    if rc == 0:
        _mark_done(queue_csv, row, "Copying", output_path)
        update_row(queue_csv, row["RowID"], {"ElapsedSeconds": str(elapsed), "ExitCode": str(rc)})
    else:
        _mark_failed(queue_csv, row, "Copying", f"COPY_{rc}", "Copy failed")
        update_row(queue_csv, row["RowID"], {"ElapsedSeconds": str(elapsed), "ExitCode": str(rc)})
