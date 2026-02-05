# -*- coding: utf-8 -*-
"""DB-API 2.0 compliant exceptions for Drill."""


class DrillWarning(Exception):
    """DB-API Warning - renamed to avoid shadowing built-in Warning."""


class Error(Exception):
    """Base class for all Drill DB-API errors."""

    def __init__(self, message, httperror):
        super().__init__(message)
        self.message = message
        self.httperror = httperror


class AuthError(Error):
    """Authentication error."""

    def __str__(self):
        return repr(f"{self.message} Authentication Error: Invalid User/Pass: {self.httperror}")


class DatabaseError(Error):
    """Database error."""

    def __str__(self):
        return repr(f"{self.message} HTTP ERROR: {self.httperror}")


class ProgrammingError(Error):
    """Programming error."""

    def __str__(self):
        return repr(f"{self.message} HTTP ERROR: {self.httperror}")


class InterfaceError(Error):
    """Interface error."""

    def __str__(self):
        return repr(f"{self.message} HTTP ERROR: {self.httperror}")


class OperationalError(Error):
    """Operational error."""

    def __str__(self):
        return repr(f"{self.message} HTTP ERROR: {self.httperror}")


class IntegrityError(Error):
    """Integrity error."""

    def __str__(self):
        return repr(f"{self.message} HTTP ERROR: {self.httperror}")


class InternalError(Error):
    """Internal error."""

    def __str__(self):
        return repr(f"{self.message} HTTP ERROR: {self.httperror}")


class NotSupportedError(Error):
    """Not supported error."""

    def __str__(self):
        return repr(f"{self.message} HTTP ERROR: {self.httperror}")


class CursorClosedException(Error):
    """Cursor closed exception."""

    def __init__(self, message):
        super().__init__(message, None)

    def __str__(self):
        return repr(self.message)


class ConnectionClosedException(Error):
    """Connection closed exception."""

    def __init__(self, message):
        super().__init__(message, None)

    def __str__(self):
        return repr(self.message)
