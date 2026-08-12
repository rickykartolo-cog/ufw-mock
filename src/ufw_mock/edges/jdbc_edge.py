from ufw_mock.edges.base import Edge


class JdbcEdge(Edge):
    """JDBC database edge node (stub for the mock)."""

    def __init__(self, connection_url: str = "") -> None:
        self.connection_url = connection_url

    def resolve(self, path: str) -> str:
        # For JDBC, `path` is interpreted as the table name.
        return path
