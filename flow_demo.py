from requests import get, post
from json import dumps
from sink import Sink
from producer import Producer
from utils import unixIntSeconds


class FlowDemo:
    def __init__(self, host, port) -> None:
        self._url = f"https://{host}:{port}"
        self._username = "demo_stream"
        self._password = "demo_stream"

        self._token = None
        self._headers = None
        self._sinks = None

        self._producer = Producer()

        self.setToken()

    # Sets self._token and self._headers
    def setToken(self):
        """Authorizes Flow Insights api with username and password."""
        url = self._url + "/users/auth"
        headers = {"Accept-Version": "2.0", "Content-Type": "application/json"}
        data = {"username": self._username, "password": self._password}

        response = post(url, dumps(data), headers=headers, verify=False)

        data = response.json()

        self._token = data["access_tokens"][0]

        self._headers = {
            "Accept-Version": "2.0",
            "Content-Type": "application/json",
            "Authorization": "Bearer " + self._token,
        }

        return response

    # Sets self._sinks
    def setSinks(self):
        """Creates list of sinks from the response to flow demo"""
        url = self._url + "/cubes/0/analytics/0/sinks"

        response = get(url, headers=self._headers, verify=False)
        data = response.json()

        sinks = [Sink(i["id"], i["output_value_type"], self) for i in data["sinks"]]
        self._sinks = sinks

        return response

    # Returns sequence_number from flow demo
    def getSequenceNumber(self):
        """FILL"""
        url = self._url + "/cubes/0/analytics/0"

        response = get(url, headers=self._headers, verify=False)

        data = response.json()

        return data["sequence_number"]

    # Sorts data and sends it to the kafka server
    def getDistribution(self, sink: Sink):
        print(f"Distribution ID: {sink._id}")
        data = sink.getHistory()

        snapshots = data["snapshots"]

        for snapshot in snapshots:
            if snapshot["data"]["data_validity"] == "ok":
                if snapshot["data_start_timestamp"] > sink._last_start_timestamp:
                    categories = snapshot["data"]["categories"]
                    for category in categories:
                        count = int(category["count"])
                        if count > 0:
                            message_data = {
                                "sensor_name": data["name"],
                                "start_timestamp": unixIntSeconds(
                                    snapshot["data_start_timestamp"]
                                ),
                                "end_timestamp": unixIntSeconds(
                                    snapshot["data_end_timestamp"]
                                ),
                                "category": category["category"],
                                "count": int(category["count"]),
                            }
                            self._producer.sendJsonMessage(
                                "data-distribution", message_data
                            )
                    sink._last_start_timestamp = snapshot["data_start_timestamp"]

    def getOriginDestination(self, sink):
        pass

    def getStatistical(self, sink):
        pass

    # Collects data from all sinks
    def run(self):
        if self._sinks == None:
            self.setSinks()

        sinks = self._sinks
        for sink in sinks:
            if sink._type == "distribution":
                self.getDistribution(sink)

            # elif sink._type == "od_matrix":
            #     self.getOriginDestination(sink)
