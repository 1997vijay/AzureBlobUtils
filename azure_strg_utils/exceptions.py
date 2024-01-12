"""
Provides customer exception for azure_strg_utils 

Usage:
    1. Just import and raised the exception as nedded.

    Author Information:
    Name: Vijay Kumar
    Date: 14 Dec 2023

Change Log:
    - 14 Dec 2023: Initial creation.
"""

class CustomError(Exception):
    """
    Base class for custom exceptions
    """
    def __init__(self, message):
        self.message = message if message else None
    
    def __str__(self) -> str:
        return str(self.message)


class EmptyColumnListError(CustomError):
    """
    Raised when an empty column list is passed to the class
    """

class EmptyFolderError(CustomError):
    """
    Raised when an empty column list is passed to the class
    """


class UploadFileError(CustomError):
    """
    Raised when an error occurs during file upload
    """


class DeleteFileError(CustomError):
    """
    Raised when an error occurs during file deletion
    """

class ConnectionError(CustomError):
    """
    Raised when an error occurs during file deletion
    """
