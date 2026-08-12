from abc import ABC, abstractmethod


class Edge(ABC):
    """Abstract edge node connector."""

    @abstractmethod
    def resolve(self, path: str) -> str:
        """Resolve a logical path to a physical location."""
