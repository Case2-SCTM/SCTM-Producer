from kafka import KafkaProducer
from json import dumps


class Producer:
    def __init__(self) -> None:
        self._kafka = None

        self.setKafkaProducer()

    def setKafkaProducer(self):
        self._kafka = KafkaProducer(
            bootstrap_servers=["localhost:9092"],
            value_serializer=lambda m: dumps(m).encode("ascii"),
        )

    def sendJsonMessage(self, topic, value):
        # produce json messages
        self._kafka.send(topic, value)
