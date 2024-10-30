class Producer:
    def __init__(self) -> None:
        self._


from requests import get, post
from json import dumps


class FlowDemo:
    def __init__(self, host, port) -> None:
        self._url = f"https://{host}:{port}"
        self._username = ""
        self._password = ""

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

        sinks = [Sink(i["id"], i["output_value_type"]) for i in data["sinks"]]
        self._sinks = sinks

        return response

    def getSequenceNumber(self):
        url = self._url + "/cubes/0/analytics/0"

        response = get(url, headers=self._headers, verify=False)


        pass

    def getDistribution(self):
        url = self._url + "/cubes/0/analytics/0/sinks/history"
        data = {"sequence_number": self.getSequenceNumber()}
        pass

    def getStatistical(self):
        pass

    def run(self):
        # Collects data from all sinks
        pass


class Sink:
    def __init__(self, id, type) -> None:
        self._id = id
        self._type = type

        pass

    def getHistory(self):
        pass

    def getData(self):
        pass
