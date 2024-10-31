from requests import get, post
from json import dumps
from sink import Sink


class FlowDemo:
    def __init__(self, host, port) -> None:
        self._url = f"https://{host}:{port}"
        self._username = "demo_stream"
        self._password = "demo_stream"

        self._token = None
        self._headers = None
        self._sinks = None

        self.setToken()

    def setToken(self):
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

    def setSinks(self):
        url = self._url + "/cubes/0/analytics/0/sinks"

        response = get(url, headers=self._headers, verify=False)
        data = response.json()

        sinks = [Sink(i["id"], i["output_value_type"], self) for i in data["sinks"]]
        self._sinks = sinks

        return response

    def getSequenceNumber(self):
        url = self._url + "/cubes/0/analytics/0"

        response = get(url, headers=self._headers, verify=False)

        data = response.json()

        return data["sequence_number"]

    def getDistribution(self, sink: Sink):
        data = sink.getHistory()

        snapshots = data["snapshots"]

        results = []
        for snapshot in snapshots:
            if snapshot["data"]["data_validity"] == "ok":
                if snapshot["data_start_timestamp"] > sink._last_start_timestamp:
                    categories = snapshot["data"]["categories"]

                    results.append(
                        {
                            "start_timestamp": snapshot["data_start_timestamp"],
                            "end_timestamp": snapshot["data_end_timestamp"],
                            "data": categories,
                        }
                    )

        print(f"Sink {sink._id}")
        print(data)

    def getOriginDestination(self, sink):
        return
        data = sink.getHistory()

        print(f"Sink {sink._id}")
        print(data)

    def getStatistical(self):
        pass

    # Collects data from all sinks
    def run(self):
        if self._sinks == None:
            self.setSinks()

        sinks = self._sinks
        for sink in sinks:
            print(sink._type)
            if sink._type == "distribution":
                self.getDistribution(sink)

            # elif sink._type == "od_matrix":
            #     self.getOriginDestination(sink)
