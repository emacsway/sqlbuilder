
__all__ = ('Error', 'MaxLengthError', 'OperatorNotFound', )


class Error(Exception):
    pass


class MaxLengthError(Error):
    pass


class OperatorNotFound(Error):
    pass
