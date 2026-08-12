from ufw_mock.edges.base import Edge


class FileEdge(Edge):
    """Filesystem-based edge node."""

    def __init__(self, base_path: str = "") -> None:
        self.base_path = base_path.rstrip("/")

    def resolve(self, path: str) -> str:
        if self.base_path:
            return f"{self.base_path}/{path.lstrip('/')}"
        return path
