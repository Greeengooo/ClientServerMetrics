import asyncio


storage = []


class ClientServerProtocol(asyncio.Protocol):
    def connection_made(self, transport):
        self.transport = transport

    def data_received(self, data):
        resp = self.process_data(data.decode())
        self.transport.write(resp.encode())

    def process_data(self, request: str) -> str:
        commands = ["get", "put"]
        request_to_list = request.strip('\n').split()
        if request_to_list[0] not in commands:
            return "error\nwrong command\n\n"
        if request_to_list[0] == "get":
            return self.perform_get(request_to_list)
        if request_to_list[0] == "put":
            return self.perform_put(request_to_list)

    def perform_get(self, data: list) -> str:
        if len(data) != 2:
            return "error\nwrong command\n\n"
        _, key = data
        if key == "*":
            return f"{self.fetch_all()}"
        return f"{self.fetch_by_key(key)}"

    def fetch_by_key(self, key) -> str:
        res = [' '.join(record) + '\n' for record in storage if record[0] == key]
        if not res:
            return "ok\n\n"
        return "ok\n" + "".join(res) + "\n\n"

    def fetch_all(self) -> str:
        if len(storage) == 0:
            return "ok\n\n"
        return "ok\n" + "".join(' '.join(i) + '\n' for i in storage) + "\n"

    def perform_put(self, data: list) -> str:
        _, name, value, timestamp = data
        if len(data) != 4 or not self.is_valid([name, value, timestamp]):
            return "error\nwrong command\n\n"
        if data in storage:
            return "error\nduplicate value\n\n"
        for i in range(len(storage)):
            if storage[i][2] == timestamp:
                storage[i] = [name, value, timestamp]
                return "ok\n\n"
        storage.append([name, value, timestamp])
        return "ok\n\n"

    def is_valid(self, values: list) -> bool:
        _, metric_value, timestamp = values
        try:
            float(metric_value)
            int(timestamp)
        except:
            return False
        return True


def run_server(host, port):
    loop = asyncio.get_event_loop()
    coro = loop.create_server(
        ClientServerProtocol,
        host,
        port
    )
    server = loop.run_until_complete(coro)
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    server.close()
    loop.run_until_complete(server.wait_closed())
    loop.close()


if __name__ == '__main__':
    run_server("127.0.0.1", 8881)