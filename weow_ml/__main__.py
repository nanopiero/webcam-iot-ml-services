"""Offline notification inspection: python3 -m weow_ml notification.json."""

import sys

from .acquisition.cli import main


if __name__ == "__main__":
    sys.exit(main())
