"""OST specific errors and warnings"""


class GPTRuntimeError(RuntimeError):
    """Raised when a GPT process returns wrong return code."""


class NotValidFile(RuntimeError):
    """Raised when an output file did not pass the validation test."""
