"""
Helper module for raising special exceptions with a custom traceback
"""
from __future__ import annotations

import sys

# subclass RuntimeError that includes the traceback
class RuntimeErrorWithTraceback(RuntimeError):
    """
    Wrapper for a function that catches exceptions and re-raises them with the
    traceback.
    """
    def __init__(self, message, exc_info):
        super().__init__(message)
        self.exc_info = exc_info

    # TODO: override __str__ to get the full traceback in the error message.
    def __reduce__(self):
        return RuntimeErrorWithTraceback, (self.args[0], self.exc_info)

    def __str__(self):
        return "<RuntimeErrorWithTraceback: %s>" % self.args[0]

    @classmethod
    def from_exc_info(cls, exc_info):
        return cls(exc_info[1], exc_info)


def raise_with_traceback(exc_type, message, exc_info):
    exc = exc_type(message)
    exc.__cause__ = RuntimeErrorWithTraceback(message, exc_info)
    raise exc
