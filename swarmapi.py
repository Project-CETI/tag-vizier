import requests


class SwarmAPI:
    def __init__(self):
        self.recent_data = {}
        self.msg_cached = False
        self.recent_telemetry = {}
        self.tel_cached = False
        self.authVal = self._login()

    def _login(self):
        url = "https://bumblebee.hive.swarm.space/hive/login"
        payload = "username=ceti&password=1pE%5ELBAO8z%24kdg%239nTQ5WnMKXERrI%5EG%24M2POTuyy9A"
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
        }
        response = requests.request("POST", url, headers=headers, data=payload)
        print("SWARM Login succeeded? %s" % str(response.status_code == 200))
        return "Bearer %s" % response.json()["token"]

    def _get_messages(self, count=100):
        url = (
            "https://bumblebee.hive.swarm.space/hive/api/v1/messages?organizationId=65466&userApplicationId=65535&devicetype=1&deviceid=0x01d27&direction=fromdevice&count=%s"
            % count
        )

        payload = {}
        headers = {"accept": "application/json", "Authorization": self.authVal}

        response = requests.request("GET", url, headers=headers, data=payload)

        if response.status_code == 200:
            self.recent_data = response.json()
            self.msg_cached = True
            return None
        print("Message GET failed.")

    def _get_telemetry(self, count=100):
        url = (
            "https://bumblebee.hive.swarm.space/hive/api/v1/telemetry/devices?organizationId=65466&srcDeviceType=1&count=%s&sortAsc=false"
            % count
        )

        payload = {}
        headers = {
            "accept": "application/json",
            "Authorization": self.authVal,
        }

        response = requests.request("GET", url, headers=headers, data=payload)
        if response.status_code == 200:
            self.recent_telemetry = response.json()
            self.tel_cached = True
            return None
        print("Telemetry GET failed.")

    def get_recent_data(self, count=100, use_cached=True):
        if not (use_cached and self.msg_cached and self.tel_cached):
            self._get_messages(count=count)
            self._get_telemetry(count=count)
            print("No caches?")
        return [self.recent_data, self.recent_telemetry]

    def logout(self):
        url = "https://bumblebee.hive.swarm.space/hive/logout"
        payload = {}
        headers = {"Authorization": self.authVal}
        response = requests.request("GET", url, headers=headers, data=payload)
        print("SWARM Logout succeeded? %s" % str(response.status_code == 204))
