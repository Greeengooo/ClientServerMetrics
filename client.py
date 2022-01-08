import time
import socket
import re
from collections import defaultdict


class Client:
    def __init__(self, host: str, port: int, timeout: int = None):
        self._host = host
        self._port = port
        self._timeout = timeout

    def send_and_fetch(self, message: str) -> str:
        with socket.create_connection((self._host, self._port), self._timeout) as sock:
            sock.sendall(message.encode("utf8"))
            server_response = sock.recv(1024)
            return server_response.decode("utf8")

    def get(self, metric_name: str) -> dict:
        server_response = self.send_and_fetch(f"get {metric_name}\n")
        if server_response == "ok\n\n":
            return {}
        if server_response == "error\nwrong command\n\n":
            raise ClientError
        response_body = self.get_response_body(server_response)
        if not self.is_valid(response_body):
            raise ClientError
        return self.create_dict_from(response_body)

    def get_response_body(self, response: str) -> list:
        return response.strip().split('\n')

    def is_valid(self, response_body: list) -> bool:
        if response_body[0] == "ok":
            return all(
                re.match(r"(.+)\s(\d+\.*\d*)\s(\d+)", i)
                for i in response_body[1:]
            )
        return False

    def create_dict_from(self, response_body: list) -> dict:
        temp = defaultdict(list)
        for elem in response_body[1:]:
            metric_name, metric_value, timestamp = elem.split()
            temp[metric_name].append((int(timestamp), float(metric_value)))
        return {key: sorted(val, key=lambda x: x[0]) for key, val in temp.items()}

    def put(self, metric_name: str, metric_value: int, timestamp: int = None) -> None:
        if timestamp is None:
            timestamp = int(time.time())
        server_response = self.send_and_fetch(f"put {metric_name} {metric_value} {timestamp}\n")
        if server_response == 'ok\n\n':
            return
        response_body = self.get_response_body(server_response)
        if not self.is_valid(response_body):
            raise ClientError
        return


class ClientError(Exception):
    pass


if __name__ == '__main__':
    cl = Client('127.0.0.1', 8881, timeout=15)
    print(cl.get('*'))
    #cl.put("k1", 0.25, timestamp=1)
