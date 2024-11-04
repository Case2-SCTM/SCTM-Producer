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
        history = sink.getHistory()

        snapshots = history["snapshots"]

        for snapshot in snapshots:
            data = snapshot["data"]
            if data["data_validity"] == "ok":
                if snapshot["data_start_timestamp"] > sink._last_start_timestamp:
                    categories = data["categories"]
                    for category in categories:
                        count = int(category["count"])
                        if count > 0:
                            message_data = {
                                "sensor_name": history["name"],
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

    def getOriginDestination(self, sink: Sink):
        print(f"Od Matrix ID: {sink._id}")
        histroy = sink.getHistory()

        snapshots = histroy["snapshots"]

        for snapshot in snapshots:
            data = snapshot["data"]
            if data["data_validity"] == "ok":
                if snapshot["data_start_timestamp"] > sink._last_start_timestamp:
                    print()

                    origins = data["origins"]
                    destinations = data["destinations"]

                    for movement in data["turning_movements"]:
                        for a, entry in enumerate(origins):
                            for b, exit in enumerate(destinations):
                                if int(movement["data"][a][b]) > 0:
                                    message = {
                                        "category": movement["category"],
                                        "entry": entry["name"],
                                        "exit": exit["name"],
                                        "count": int(movement["data"][a][b]),
                                        "timestamp": unixIntSeconds(
                                            snapshot["data_start_timestamp"]
                                        ),
                                    }

                                    self._producer.sendJsonMessage(
                                        "data-odmatrix", message
                                    )
                    sink._last_start_timestamp = snapshot["data_start_snapshot"]

    def getStatistical(self, sink):
        pass

    # Collects data from all sinks
    def run(self):
        if self._sinks == None:
            self.setSinks()

        sinks = self._sinks
        for sink in sinks:
            if sink._type == "distribution":
                print()
                # self.getDistribution(sink)

            elif sink._type == "od_matrix":
                self.getOriginDestination(sink)
