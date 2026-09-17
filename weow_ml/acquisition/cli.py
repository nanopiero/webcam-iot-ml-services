"""Offline notification inspection: python3 -m weow_ml notification.json."""

import argparse
import json
import sys
from pathlib import Path

from .contracts import ContractError, parse_notification


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate an ingestion notification without publishing jobs")
    parser.add_argument("notification", type=Path)
    args = parser.parse_args()
    try:
        result = parse_notification(json.loads(args.notification.read_text()))
    except (OSError, ValueError, ContractError) as exc:
        print(f"Notification rejected: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(result.summary(), indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
