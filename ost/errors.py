"""Errors and Warnings."""


class OSTConfigError(ValueError):
    """Raised when a OST process configuration is invalid."""


class GPTRuntimeError(RuntimeError):
    """Raised when a GPT process returns wrong return code."""
