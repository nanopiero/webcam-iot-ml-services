"""Compatibility entry point for the bounded archive diagnostic."""

import sys

from .acquisition.archive import archive_notification, main, put_verified, read_object, s3_client

__all__ = ("archive_notification", "put_verified", "read_object", "s3_client")


if __name__ == "__main__":
    sys.exit(main())
