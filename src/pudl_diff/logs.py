"""Logging for the diff tool, using only the standard library."""

import logging

LOGGER_NAME = "pudl_diff"
"""The parent of the loggers of all this tool's modules, so that an application
using the tool can configure its logging in one place."""


def get_logger(module_name: str) -> logging.Logger:
    """The logger for a module, named ``pudl_diff.<module>``.

    Args:
        module_name: The module's ``__name__``. Only its last component is used, so
            that the loggers' names don't depend on where the tool is installed.
    """
    return logging.getLogger(f"{LOGGER_NAME}.{module_name.rpartition('.')[2]}")
