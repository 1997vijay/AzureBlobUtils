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

class EmptyColumnListError(Exception):
    """
    Raise when empty column list passed to the class
    """
    def __init__(self,message):
        if message:
            self.message=message
        else:
            self.message=None
    
    def __str__(self) -> str:
       if self.message:
           return self.message


