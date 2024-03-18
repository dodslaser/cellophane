"""Exceptions for the data module"""

class MergeSamplesTypeError(Exception):
    """Raised when trying to merge samples of different types"""

    msg: str = "Cannot merge samples of different types"


class MergeSamplesUUIDError(Exception):
    """Raised when trying to merge samples with different UUIDs"""

    msg: str = "Cannot merge samples with different UUIDs"

