from datetime import datetime, timedelta
from json import dumps
from requests import post


class Sink:
    def __init__(self, id, type, flow) -> None:
        self._id = id
        self._type = type
        self._flow = flow
        self._url = self._flow._url + "/cubes/0/analytics/0/sinks"

        self._last_start_timestamp = str(int((datetime.now() - timedelta(hours=8)).timestamp() * 1000))

    def getHistory(self):
        url = self._url + "/history"
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

        response = post(url, dumps(data), headers=self._flow._headers, verify=False)

        data = response.json()

        return data["sinks"][0]

    def getData(self):
        pass
