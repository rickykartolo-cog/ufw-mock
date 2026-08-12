from ufw_mock.edges.base import Edge
from ufw_mock.edges.file_edge import FileEdge
from ufw_mock.edges.jdbc_edge import JdbcEdge
from ufw_mock.edges.kafka_edge import KafkaEdge
from ufw_mock.edges.rest_edge import RestEdge
from ufw_mock.types import EdgeProtocol

_EDGES: dict[EdgeProtocol, type[Edge]] = {
    EdgeProtocol.FILE: FileEdge,
    EdgeProtocol.KAFKA: KafkaEdge,
    EdgeProtocol.REST: RestEdge,
    EdgeProtocol.JDBC: JdbcEdge,
}


def get_edge(protocol: EdgeProtocol, properties: dict | None = None) -> Edge:
    try:
        return _EDGES[protocol](**properties or {})
    except KeyError as exc:
        raise ValueError(f"Unsupported edge protocol: {protocol}") from exc


__all__ = [
    "Edge",
    "FileEdge",
    "KafkaEdge",
    "RestEdge",
    "JdbcEdge",
    "get_edge",
]
