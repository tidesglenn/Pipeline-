from __future__ import annotations

import tkinter as tk

from .settings import load_settings
from .ui import ArchiveToolApp


def main() -> None:
    root = tk.Tk()
    settings = load_settings()
    ArchiveToolApp(root, settings)
    root.mainloop()


if __name__ == "__main__":
    main()
