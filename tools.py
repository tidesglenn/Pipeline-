from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path
from typing import Dict, List


def build_7zip_command(row: Dict[str, str]) -> List[str]:
    tool_path = row.get("ToolPath", "")
    source_path = row.get("SourcePath", "")
    final_destination = row.get("FinalDestination", "")
    options = json.loads(row.get("OptionsJson", "{}") or "{}")
    args = [tool_path, "x", source_path, f"-o{final_destination}"]
    if options.get("overwrite_existing", False):
        args.append("-y")
    if options.get("test_before_extract", False):
        # real implementation would run a test command first
        pass
    return args


def run_7zip(row: Dict[str, str], log_path: str) -> subprocess.CompletedProcess:
    command = build_7zip_command(row)
    Path(log_path).parent.mkdir(parents=True, exist_ok=True)
    with open(log_path, "a", encoding="utf-8") as log_handle:
        log_handle.write("COMMAND: " + subprocess.list2cmdline(command) + "\n")
        process = subprocess.run(command, stdout=log_handle, stderr=log_handle, text=True)
    return process


def run_python_copy(row: Dict[str, str], log_path: str) -> int:
    source = row.get("SourcePath", "")
    dest = row.get("CopyDestination", "") or row.get("FinalDestination", "")
    Path(log_path).parent.mkdir(parents=True, exist_ok=True)
    with open(log_path, "a", encoding="utf-8") as log_handle:
        log_handle.write(f"COPY {source} -> {dest}\n")
    try:
        if os.path.isdir(source):
            shutil.copytree(source, dest, dirs_exist_ok=True)
        else:
            Path(dest).parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, dest)
        return 0
    except Exception as exc:
        with open(log_path, "a", encoding="utf-8") as log_handle:
            log_handle.write(f"ERROR: {exc}\n")
        return 1
