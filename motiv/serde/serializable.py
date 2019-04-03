"""Interfaces for serializable types"""

import abc


class Serializable(abc.ABC):
    """
    A De/Serializable interface
    """
    @abc.abstractmethod
    def serialize(self):
        """returns encoded object"""

    @abc.abstractmethod
    def deserialize(self):
        """decodes an object into an instance"""
