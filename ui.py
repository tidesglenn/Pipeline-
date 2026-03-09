from __future__ import annotations

import os
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from typing import Dict, List

from .csv_store import append_rows, load_queue_csv, save_queue_csv, update_row
from .enumerators import enumerate_copy_jobs, enumerate_unzip_jobs
from .models import AppSettings
from .progress import summarize_rows
from .scheduler import QueueManager
from .validators import detect_duplicate_destinations, validate_copy_row, validate_unzip_row


class ArchiveToolApp(ttk.Frame):
    def __init__(self, master: tk.Tk, settings: AppSettings) -> None:
        super().__init__(master)
        self.master = master
        self.settings = settings
        self.unzip_manager: QueueManager | None = None
        self.copy_manager: QueueManager | None = None
        self.pack(fill="both", expand=True)
        self._build_ui()
        self._refresh_all_views()

    def _build_ui(self) -> None:
        self.master.title("Archive Queue Tool")
        self.master.geometry("1280x760")
        notebook = ttk.Notebook(self)
        notebook.pack(fill="both", expand=True)

        self.unzip_tab = ttk.Frame(notebook)
        self.copy_tab = ttk.Frame(notebook)
        self.monitor_tab = ttk.Frame(notebook)
        notebook.add(self.unzip_tab, text="Unzip")
        notebook.add(self.copy_tab, text="Copy")
        notebook.add(self.monitor_tab, text="Monitor")

        self._build_unzip_tab()
        self._build_copy_tab()
        self._build_monitor_tab()

    def _build_unzip_tab(self) -> None:
        top = ttk.Frame(self.unzip_tab, padding=10)
        top.pack(fill="x")
        self.unzip_source_var = tk.StringVar()
        self.unzip_dest_var = tk.StringVar()
        self.unzip_csv_var = tk.StringVar(value=self.settings.unzip_queue_csv)
        self.seven_zip_var = tk.StringVar(value=self.settings.seven_zip_path)
        self.unzip_jobs_var = tk.IntVar(value=self.settings.unzip_parallel_jobs)
        self.unzip_include_subfolders = tk.BooleanVar(value=True)
        self.unzip_overwrite = tk.BooleanVar(value=True)
        self.unzip_test = tk.BooleanVar(value=False)

        self._labeled_entry(top, 0, "Source", self.unzip_source_var, self._browse_unzip_source)
        self._labeled_entry(top, 1, "Destination", self.unzip_dest_var, self._browse_unzip_destination)
        self._labeled_entry(top, 2, "Queue CSV", self.unzip_csv_var, self._browse_unzip_csv, save=True)
        self._labeled_entry(top, 3, "7-Zip EXE", self.seven_zip_var, self._browse_7zip)
        ttk.Label(top, text="Jobs running at once").grid(row=4, column=0, sticky="w", padx=4, pady=4)
        ttk.Spinbox(top, from_=1, to=32, textvariable=self.unzip_jobs_var, width=8).grid(row=4, column=1, sticky="w", padx=4, pady=4)

        options = ttk.Frame(top)
        options.grid(row=5, column=0, columnspan=3, sticky="w", padx=4, pady=4)
        ttk.Checkbutton(options, text="Include subfolders", variable=self.unzip_include_subfolders).pack(side="left", padx=4)
        ttk.Checkbutton(options, text="Overwrite existing", variable=self.unzip_overwrite).pack(side="left", padx=4)
        ttk.Checkbutton(options, text="Test archive before extract", variable=self.unzip_test).pack(side="left", padx=4)

        buttons = ttk.Frame(top)
        buttons.grid(row=6, column=0, columnspan=3, sticky="w", padx=4, pady=8)
        ttk.Button(buttons, text="Enumerate", command=self.on_enumerate_unzip).pack(side="left", padx=4)
        ttk.Button(buttons, text="Validate", command=self.on_validate_unzip).pack(side="left", padx=4)
        ttk.Button(buttons, text="Start", command=self.on_start_unzip).pack(side="left", padx=4)
        ttk.Button(buttons, text="Refresh", command=self.refresh_unzip_table).pack(side="left", padx=4)

        self.unzip_tree = self._build_tree(self.unzip_tab)

    def _build_copy_tab(self) -> None:
        top = ttk.Frame(self.copy_tab, padding=10)
        top.pack(fill="x")
        self.copy_source_var = tk.StringVar()
        self.copy_dest_var = tk.StringVar()
        self.copy_csv_var = tk.StringVar(value=self.settings.copy_queue_csv)
        self.copy_jobs_var = tk.IntVar(value=self.settings.copy_parallel_jobs)
        self.copy_include_subfolders = tk.BooleanVar(value=True)
        self.copy_by_child = tk.BooleanVar(value=True)

        self._labeled_entry(top, 0, "Source", self.copy_source_var, self._browse_copy_source)
        self._labeled_entry(top, 1, "Destination", self.copy_dest_var, self._browse_copy_destination)
        self._labeled_entry(top, 2, "Queue CSV", self.copy_csv_var, self._browse_copy_csv, save=True)
        ttk.Label(top, text="Jobs running at once").grid(row=3, column=0, sticky="w", padx=4, pady=4)
        ttk.Spinbox(top, from_=1, to=32, textvariable=self.copy_jobs_var, width=8).grid(row=3, column=1, sticky="w", padx=4, pady=4)

        options = ttk.Frame(top)
        options.grid(row=4, column=0, columnspan=3, sticky="w", padx=4, pady=4)
        ttk.Checkbutton(options, text="Include subfolders", variable=self.copy_include_subfolders).pack(side="left", padx=4)
        ttk.Checkbutton(options, text="Queue immediate child items", variable=self.copy_by_child).pack(side="left", padx=4)

        buttons = ttk.Frame(top)
        buttons.grid(row=5, column=0, columnspan=3, sticky="w", padx=4, pady=8)
        ttk.Button(buttons, text="Enumerate", command=self.on_enumerate_copy).pack(side="left", padx=4)
        ttk.Button(buttons, text="Validate", command=self.on_validate_copy).pack(side="left", padx=4)
        ttk.Button(buttons, text="Start", command=self.on_start_copy).pack(side="left", padx=4)
        ttk.Button(buttons, text="Refresh", command=self.refresh_copy_table).pack(side="left", padx=4)

        self.copy_tree = self._build_tree(self.copy_tab)

    def _build_monitor_tab(self) -> None:
        frame = ttk.Frame(self.monitor_tab, padding=12)
        frame.pack(fill="both", expand=True)
        self.monitor_summary_var = tk.StringVar(value="No queue loaded")
        ttk.Label(frame, textvariable=self.monitor_summary_var, font=("Segoe UI", 11, "bold")).pack(anchor="w")
        ttk.Button(frame, text="Refresh All", command=self._refresh_all_views).pack(anchor="w", pady=8)
        self.monitor_text = tk.Text(frame, height=24, width=120)
        self.monitor_text.pack(fill="both", expand=True)

    def _build_tree(self, parent: ttk.Frame) -> ttk.Treeview:
        columns = ("RowID", "JobName", "SourcePath", "FinalDestination", "Status", "PercentComplete", "ValidationMessage", "LastMessage")
        frame = ttk.Frame(parent, padding=(10, 0, 10, 10))
        frame.pack(fill="both", expand=True)
        tree = ttk.Treeview(frame, columns=columns, show="headings")
        for col in columns:
            tree.heading(col, text=col)
            tree.column(col, width=140, anchor="w")
        tree.pack(side="left", fill="both", expand=True)
        scroll = ttk.Scrollbar(frame, orient="vertical", command=tree.yview)
        scroll.pack(side="right", fill="y")
        tree.configure(yscrollcommand=scroll.set)
        return tree

    def _labeled_entry(self, parent: ttk.Frame, row: int, label: str, variable: tk.StringVar, browse_cmd, save: bool = False) -> None:
        ttk.Label(parent, text=label).grid(row=row, column=0, sticky="w", padx=4, pady=4)
        ttk.Entry(parent, textvariable=variable, width=100).grid(row=row, column=1, sticky="ew", padx=4, pady=4)
        ttk.Button(parent, text="Browse", command=browse_cmd).grid(row=row, column=2, sticky="w", padx=4, pady=4)
        parent.columnconfigure(1, weight=1)

    def _browse_unzip_source(self) -> None:
        path = filedialog.askopenfilename() or filedialog.askdirectory()
        if path:
            self.unzip_source_var.set(path)

    def _browse_unzip_destination(self) -> None:
        path = filedialog.askdirectory()
        if path:
            self.unzip_dest_var.set(path)

    def _browse_unzip_csv(self) -> None:
        path = filedialog.asksaveasfilename(defaultextension=".csv")
        if path:
            self.unzip_csv_var.set(path)

    def _browse_7zip(self) -> None:
        path = filedialog.askopenfilename(filetypes=[("Executable", "*.exe"), ("All files", "*.*")])
        if path:
            self.seven_zip_var.set(path)

    def _browse_copy_source(self) -> None:
        path = filedialog.askopenfilename() or filedialog.askdirectory()
        if path:
            self.copy_source_var.set(path)

    def _browse_copy_destination(self) -> None:
        path = filedialog.askdirectory()
        if path:
            self.copy_dest_var.set(path)

    def _browse_copy_csv(self) -> None:
        path = filedialog.asksaveasfilename(defaultextension=".csv")
        if path:
            self.copy_csv_var.set(path)

    def on_enumerate_unzip(self) -> None:
        rows = load_queue_csv(self.unzip_csv_var.get())
        new_rows = enumerate_unzip_jobs(
            self.unzip_source_var.get(),
            self.unzip_dest_var.get(),
            rows,
            self.seven_zip_var.get(),
            self.unzip_include_subfolders.get(),
            self.unzip_overwrite.get(),
            self.unzip_test.get(),
        )
        append_rows(self.unzip_csv_var.get(), new_rows)
        self.refresh_unzip_table()
        messagebox.showinfo("Enumerate", f"Added {len(new_rows)} unzip rows")

    def on_enumerate_copy(self) -> None:
        rows = load_queue_csv(self.copy_csv_var.get())
        new_rows = enumerate_copy_jobs(
            self.copy_source_var.get(),
            self.copy_dest_var.get(),
            rows,
            self.copy_include_subfolders.get(),
            self.copy_by_child.get(),
        )
        append_rows(self.copy_csv_var.get(), new_rows)
        self.refresh_copy_table()
        messagebox.showinfo("Enumerate", f"Added {len(new_rows)} copy rows")

    def on_validate_unzip(self) -> None:
        rows = load_queue_csv(self.unzip_csv_var.get())
        duplicates = set(detect_duplicate_destinations(rows, "FinalDestination"))
        for row in rows:
            ok, msg = validate_unzip_row(row)
            if row.get("RowID", "") in duplicates:
                ok = False
                msg = "Duplicate destination detected"
            row["Validated"] = "Yes" if ok else "No"
            row["ValidationMessage"] = msg
            row["Status"] = "Ready" if ok else "Draft"
        save_queue_csv(self.unzip_csv_var.get(), rows)
        self.refresh_unzip_table()

    def on_validate_copy(self) -> None:
        rows = load_queue_csv(self.copy_csv_var.get())
        duplicates = set(detect_duplicate_destinations(rows, "CopyDestination"))
        for row in rows:
            ok, msg = validate_copy_row(row)
            if row.get("RowID", "") in duplicates:
                ok = False
                msg = "Duplicate destination detected"
            row["Validated"] = "Yes" if ok else "No"
            row["ValidationMessage"] = msg
            row["Status"] = "Ready" if ok else "Draft"
        save_queue_csv(self.copy_csv_var.get(), rows)
        self.refresh_copy_table()

    def on_start_unzip(self) -> None:
        self.unzip_manager = QueueManager(self.unzip_csv_var.get(), self.settings.log_dir, int(self.unzip_jobs_var.get()), "Unzip")
        self.unzip_manager.start()
        self.master.after(self.settings.refresh_ms, self._refresh_all_views)

    def on_start_copy(self) -> None:
        self.copy_manager = QueueManager(self.copy_csv_var.get(), self.settings.log_dir, int(self.copy_jobs_var.get()), "Copy")
        self.copy_manager.start()
        self.master.after(self.settings.refresh_ms, self._refresh_all_views)

    def refresh_unzip_table(self) -> None:
        self._fill_tree(self.unzip_tree, load_queue_csv(self.unzip_csv_var.get()))

    def refresh_copy_table(self) -> None:
        self._fill_tree(self.copy_tree, load_queue_csv(self.copy_csv_var.get()))

    def _fill_tree(self, tree: ttk.Treeview, rows: List[Dict[str, str]]) -> None:
        for item in tree.get_children():
            tree.delete(item)
        for row in rows:
            tree.insert(
                "",
                "end",
                values=(
                    row.get("RowID", ""),
                    row.get("JobName", ""),
                    row.get("SourcePath", ""),
                    row.get("FinalDestination", "") or row.get("CopyDestination", ""),
                    row.get("Status", ""),
                    row.get("PercentComplete", ""),
                    row.get("ValidationMessage", ""),
                    row.get("LastMessage", ""),
                ),
            )

    def _refresh_all_views(self) -> None:
        self.refresh_unzip_table()
        self.refresh_copy_table()
        unzip_rows = load_queue_csv(self.unzip_csv_var.get())
        copy_rows = load_queue_csv(self.copy_csv_var.get())
        uz = summarize_rows(unzip_rows)
        cp = summarize_rows(copy_rows)
        summary = (
            f"Unzip total={uz['total']} running={uz['running']} complete={uz['completed']} failed={uz['failed']} | "
            f"Copy total={cp['total']} running={cp['running']} complete={cp['completed']} failed={cp['failed']}"
        )
        self.monitor_summary_var.set(summary)
        self.monitor_text.delete("1.0", "end")
        self.monitor_text.insert("end", summary + "\n\n")
        self.monitor_text.insert("end", "UNZIP QUEUE\n")
        for row in unzip_rows[:50]:
            self.monitor_text.insert("end", f"{row.get('RowID')} | {row.get('Status')} | {row.get('LastMessage')}\n")
        self.monitor_text.insert("end", "\nCOPY QUEUE\n")
        for row in copy_rows[:50]:
            self.monitor_text.insert("end", f"{row.get('RowID')} | {row.get('Status')} | {row.get('LastMessage')}\n")
        self.master.after(self.settings.refresh_ms, self._refresh_all_views)
