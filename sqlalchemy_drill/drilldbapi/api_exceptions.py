# -*- coding: utf-8 -*-


class Warning(Exception):
    pass


class Error(Exception):
    def __init__(self, message, httperror):
        self.message = message
        self.httperror = httperror


class AuthError(Error):

    def __str__(self):
        return repr(
            "{msg} {type} {err}".format(
                msg=self.message,
                type="Authentication Error: Invalid User/Pass:",
                err=self.httperror,
            )
        )


class DatabaseError(Error):

    def __str__(self):
        return repr(
            "{msg} {type} {err}".format(
                msg=self.message,
                type="HTTP ERROR:",
                err=self.httperror,
            )
        )


class ProgrammingError(Error):

    def __str__(self):
        return repr(
            "{msg} {type} {err}".format(
                msg=self.message,
                type="HTTP ERROR:",
                err=self.httperror,
            )
        )


class CursorClosedException(Error):

    def __init__(self, message):
        self.message = message

    def __str__(self):
        return repr(self.message)


class ConnectionClosedException(Error):

    def __init__(self, message):
        self.message = message

    def __str__(self):
        return repr(self.message)
