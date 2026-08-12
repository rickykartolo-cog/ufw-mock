from ufw_mock.edges.base import Edge


class KafkaEdge(Edge):
    """Kafka/Confluent edge node (stub for the mock)."""

    def __init__(self, bootstrap_servers: str = "localhost:9092") -> None:
        self.bootstrap_servers = bootstrap_servers

    def resolve(self, path: str) -> str:
        # For Kafka, `path` is interpreted as the topic name.
        return path
