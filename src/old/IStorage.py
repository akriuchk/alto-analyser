from abc import ABC, abstractmethod
from typing import Iterable

from old.models import Token


class IStorage(ABC):
    @abstractmethod
    def insert_one(self, data: Token) -> None:
        """Insert a single document into the database."""
        pass

    @abstractmethod
    def insert_many(self, data: list[Token]) -> None:
        """Insert multiple documents into the database."""
        pass

    @abstractmethod
    def find_shallow(self, token: str) -> Token:
        """Retrieve documents based on a query."""
        pass

    @abstractmethod
    def update(self, token: Token) -> None:
        """Update document"""
        pass

    @abstractmethod
    def update_many(self, tokens: Iterable[Token]) -> None:
        """Update documents"""
        pass

    @abstractmethod
    def delete_all(self) -> None:
        """Delete all documents (for testing purposes)."""
        pass

    @abstractmethod
    def save_many(self, tokens: Iterable[Token]) -> None:
        """Insert or update entries"""
        pass


    @abstractmethod
    def find_all(self) -> dict[str, Token]:
        pass