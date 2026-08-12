from ufw_mock.edges import Edge, get_edge
from ufw_mock.models.edge_node import EdgeNode


class EdgeNodeRegistry:
    """Registry that maps logical edge node names to concrete Edge connectors."""

    def __init__(self, edge_nodes: list[EdgeNode] | None = None) -> None:
        self._nodes: dict[str, EdgeNode] = {}
        for node in edge_nodes or []:
            self._nodes[node.name] = node

    def get_edge(self, name: str) -> Edge:
        if name not in self._nodes:
            # Fallback to a simple file edge when the edge node is not declared.
            return get_edge_from_name(name)
        node = self._nodes[name]
        return get_edge(node.protocol, node.properties)


def get_edge_from_name(name: str) -> Edge:
    """Fallback edge resolver for undeclared edge nodes."""
    from ufw_mock.edges import FileEdge

    return FileEdge()
