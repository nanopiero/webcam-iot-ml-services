"""Compatibility entry point for the MQTT contract diagnostic."""

import sys

from .acquisition.capture import capture, main, mqtt, time

__all__ = ("capture", "main")


if __name__ == "__main__":
    sys.exit(main())
