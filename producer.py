from kafka import KafkaProducer
from json import dumps


# Producer:
# Handles all kafka operations
class Producer:
    def __init__(self) -> None:
        self._kafka = None

        self.setKafkaProducer()

    # Creates a object of the class KafkaProducer and sets self._kafka
    def setKafkaProducer(self):
        # Bootstrap_server is set to the ip address where Apache Kafka is running on.
        # Value_serializer is set, because we want to send json as the message.
        self._kafka = KafkaProducer(
            bootstrap_servers=["localhost:9092"],
            value_serializer=lambda m: dumps(m).encode("ascii"),
        )

    # Sends a json object to choosen topic
    def sendJsonMessage(self, topic, value):
        print(f"Sent data to topic: {topic}")
        # Uses self._kafka to produce a json message in the choosen topic.
        self._kafka.send(topic, value)
