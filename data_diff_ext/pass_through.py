"""Passthrough shim for DataDiff extension."""
import sys

import structlog

from data_diff_ext.extension import DataDiff
from meltano.edk.logging import pass_through_logging_config


def pass_through_cli() -> None:
    """Pass through CLI entry point."""
    pass_through_logging_config()
    ext = DataDiff()
    ext.pass_through_invoker(
        structlog.getLogger("data_diff_invoker"),
        *sys.argv[1:] if len(sys.argv) > 1 else []
    )
