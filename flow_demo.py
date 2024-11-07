from requests import get, post
from json import dumps
from sink import Sink
from producer import Producer
from utils import unixIntSeconds


# FlowDemo:
# Handles Api calls to our flow demo, and uses a Producer object to send data onward to the topics
class FlowDemo:
    def __init__(self, host, port) -> None:
        self._url = f"https://{host}:{port}"
        self._username = "demo_stream"
        self._password = "demo_stream"

        self._headers = {"username": self._username, "password": self._password}
        self._token = None
        self._sinks = None

        self._producer = Producer()

        self.setToken()

    # Sets self._token and self._headers
    def setToken(self):
        """Authorizes Flow Insights api with username and password."""
        url = self._url + "/users/auth"
        data = {"username": self._username, "password": self._password}

        # Sends post request to url, together with username and password
        response = post(url, dumps(data), headers=self._headers, verify=False)

        # set data to response's json
        data = response.json()

        # Sets self._token to the access_token from the response's json
        self._token = data["access_tokens"][0]

        # Updates self._headers with "Authorization": "Bearer" + the access_token
        self._headers = {
            "Accept-Version": "2.0",
            "Content-Type": "application/json",
            "Authorization": "Bearer " + self._token,
        }

        # Returns response
        return response

    # Sets self._sinks
    def setSinks(self):
        """Creates list of sinks from the response to flow demo"""
        url = self._url + "/cubes/0/analytics/0/sinks"

        # Sends get request to url, together with headers
        response = get(url, headers=self._headers, verify=False)
        data = response.json()

        # Creates a object of our Sink class for each sink, and append objects to self._sinks
        sinks = [Sink(i["id"], i["output_value_type"], self) for i in data["sinks"]]
        self._sinks = sinks

        # Return response
        return response

    # Returns sequence_number from flow demo
    def getSequenceNumber(self):
        url = self._url + "/cubes/0/analytics/0"

        # Send get request to url, with headers
        response = get(url, headers=self._headers, verify=False)

        data = response.json()

        # Returns sequence_number, from response's json
        return data["sequence_number"]

    # Sorts data and sends it to the kafka server
    def getDistribution(self, sink: Sink):
        print(f"Distribution ID: {sink._id}")

        # Runs Sink's getHistory function
        history = sink.getHistory()

        # Get snapshots from history
        snapshots = history["snapshots"]

        # Runs through each snapshot in snapshots
        for snapshot in snapshots:
            # Get data from snapshot
            data = snapshot["data"]

            # Check if snapshot's data_validity is "ok"
            if data["data_validity"] == "ok":

                # Checks if snapshot is newer then last processed snapshot
                if snapshot["data_start_timestamp"] > sink._last_start_timestamp:

                    # Get categories from snapshot's data
                    categories = data["categories"]

                    # Runs through each category
                    for category in categories:
                        count = int(category["count"])

                        # Sorts out categorys where there is no relevent data
                        if count > 0:
                            # Creates the json/dict object that will be send to a topic
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

                            # Uses self._producer to send message to topic
                            self._producer.sendJsonMessage(
                                "data-distribution", message_data
                            )

                    # Saves last snapshot timestamp
                    sink._last_start_timestamp = snapshot["data_start_timestamp"]

    # Collects data from all sinks
    def run(self):
        if self._sinks == None:
            self.setSinks()

        sinks = self._sinks
        # Runs through each sink in sinks
        for sink in sinks:
            if sink._type == "distribution":
                # Runs getDistribution, that sends the data to a topic
                self.getDistribution(sink)
