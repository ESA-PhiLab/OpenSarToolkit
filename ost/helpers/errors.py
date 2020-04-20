"""OST specific errors and warnings"""


class GPTRuntimeError(RuntimeError):
    """Raised when a GPT process returns wrong return code."""
    def __init__(self, message):
        self.message = message


class NotValidFileError(RuntimeError):
    """Raised when an output file did not pass the validation test."""
    def __init__(self, message):
        self.message = message

