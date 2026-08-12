from ufw_mock.edges.base import Edge


class RestEdge(Edge):
    """REST API edge node (stub for the mock)."""

    def __init__(self, base_url: str = "") -> None:
        self.base_url = base_url.rstrip("/")

    def resolve(self, path: str) -> str:
        if self.base_url:
            return f"{self.base_url}/{path.lstrip('/')}"
        return path
