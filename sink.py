from datetime import datetime, timedelta
from json import dumps
from requests import post


# Sink:
# Handles all api calls to the sinks in our flow demo
class Sink:
    def __init__(self, id, type, flow) -> None:
        self._id = id
        self._type = type
        self._flow = flow
        self._url = self._flow._url + "/cubes/0/analytics/0/sinks"

        self._last_start_timestamp = str(
            int((datetime.now() - timedelta(hours=8)).timestamp() * 1000)
        )

    # Calls our flow demo's endpoint to get data from sink.
    def getHistory(self):
        # Whole Url: "https://{ip}:{port}/cubes/0/analytics/0/sinks/history"
        url = self._url + "/history"

        # Gets data between "start_timestamp" and "end_timestamp"
        # Timestamps has to be a string of a UNIX_TIMESTAMP in milliseconds
        # Example: (string) "1730969761221"
        data = {
            "sequence_number": self._flow.getSequenceNumber(),
            "sinks": [
                {
                    "id": self._id,
                    "start_timestamp": self._last_start_timestamp,
                    "end_timestamp": str(int(datetime.now().timestamp() * 1000)),
                }
            ],
        }

        # Send a post request to url, together with data and headers.
        response = post(url, dumps(data), headers=self._flow._headers, verify=False)

        # Get json object from response
        data = response.json()

        # Return data collected from sink
        return data["sinks"][0]
