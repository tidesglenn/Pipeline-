from __future__ import annotations

import queue
import threading
from typing import Callable, Dict, List

from .csv_store import load_queue_csv, update_row
from .workers import process_copy_row, process_unzip_row


class QueueManager:
    def __init__(self, queue_csv: str, log_dir: str, max_parallel_jobs: int, job_action: str) -> None:
        self.queue_csv = queue_csv
        self.log_dir = log_dir
        self.max_parallel_jobs = max_parallel_jobs
        self.job_action = job_action
        self._stop_event = threading.Event()
        self._threads: List[threading.Thread] = []
        self._queue: "queue.Queue[Dict[str, str]]" = queue.Queue()

    def load_ready_rows(self) -> None:
        rows = load_queue_csv(self.queue_csv)
        for row in rows:
            if row.get("Run", "") != "Yes":
                continue
            if row.get("Validated", "") != "Yes":
                continue
            if row.get("Status", "") in {"Completed", "Running"}:
                continue
            update_row(self.queue_csv, row["RowID"], {"Status": "Queued", "LastMessage": "Queued for run"})
            self._queue.put(row)

    def start(self) -> None:
        self.load_ready_rows()
        for _ in range(max(1, self.max_parallel_jobs)):
            thread = threading.Thread(target=self._worker_loop, daemon=True)
            thread.start()
            self._threads.append(thread)

    def stop(self) -> None:
        self._stop_event.set()

    def _worker_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                row = self._queue.get_nowait()
            except queue.Empty:
                return
            try:
                if self.job_action == "Unzip":
                    process_unzip_row(self.queue_csv, row, self.log_dir)
                elif self.job_action == "Copy":
                    process_copy_row(self.queue_csv, row, self.log_dir)
            finally:
                self._queue.task_done()
