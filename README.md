# Archive Tool Skeleton

This is a Python skeleton for a Windows analyst-facing archive/copy queue tool.

## What is included

- `archive_tool/main.py` - app entry point
- `archive_tool/ui.py` - Tkinter tabs for Unzip, Copy, and Monitor
- `archive_tool/csv_store.py` - CSV queue persistence
- `archive_tool/enumerators.py` - source folder enumeration into queue rows
- `archive_tool/validators.py` - row validation
- `archive_tool/scheduler.py` - queue runner with parallel job count
- `archive_tool/workers.py` - unzip and copy workers
- `archive_tool/tools.py` - external tool wrappers
- `archive_tool/progress.py` - stats and simple summaries
- `archive_tool/settings.py` - JSON settings load/save
- `archive_tool/models.py` - constants and row definitions

## Run

```bash
python -m archive_tool.main
```

## Notes

- This is a working skeleton, not a finished production build.
- Unzip uses 7-Zip.
- Copy currently uses Python copy logic as a simple starting point.
- `CopyThenUnzip` is not wired yet but the model is ready for it.
- ETA and measured percent logic are placeholders for the next phase.
