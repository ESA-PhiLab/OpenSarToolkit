"""Errors and Warnings."""


class OSTAuthenticationError(ValueError):
    """Raised when a somwthing is wrong with your credentials."""


class OSTConfigError(ValueError):
    """Raised when a OST process configuration is invalid."""


class GPTRuntimeError(RuntimeError):
    """Raised when a GPT process returns wrong return code."""
