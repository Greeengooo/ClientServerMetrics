import sys
import pytest
from client_metrics import Client, ClientError


client1 = Client("127.0.0.1", 8881, timeout=5)
client2 = Client("127.0.0.1", 8881, timeout=5)


def test_client_wrong():
    command = "wrong command test\n"
    try:
        data = client1.get(command)
    except ClientError:
        pass
    except BaseException as err:
        print(f"No connection: {err.__class__}: {err}")
        sys.exit(1)
    else:
        print("Wrong command")
        sys.exit(1)


def test_some_key():
    command = 'some_key'
    try:
        data_1 = client1.get(command)
        data_2 = client1.get(command)
    except ClientError:
        print('Server returned a result for the request that is is invalid in client.. ')
    except BaseException as err:
        sys.exit(1)

    assert data_1 == data_2 == {}

def test_put():
    try:
        client1.put("k1", 0.25, timestamp=1)
        client2.put("k1", 2.156, timestamp=2)
        client1.put("k1", 0.35, timestamp=3)
        client2.put("k2", 30, timestamp=4)
        client1.put("k2", 40, timestamp=5)
        client1.put("k2", 41, timestamp=5)
    except Exception as err:
        print(f"Error when calling client.put(...) {err.__class__}: {err}")
        sys.exit(1)


def test_get_all():
    expected_metrics = {
        "k1": [(1, 0.25), (2, 2.156), (3, 0.35)],
        "k2": [(4, 30.0), (5, 41.0)],
    }
    try:
        metrics = client1.get("*")
        if metrics != expected_metrics:
            print(f"client.get('*') returned wronf result. Waiting: "
                  f"{expected_metrics}. Received: {metrics}")
            sys.exit(1)
    except Exception as err:
        print(f"Error when calling client.get('*') {err.__class__}: {err}")
        sys.exit(1)


def test_get_none():
    try:
        metrics = client1.get('\n')
        sys.exit(1)
    except Exception as err:
        pass


def test_get_key1():
    expected_metrics = {"k2": [(4, 30.0), (5, 41.0)]}
    try:
        metrics = client2.get("k2")
        if metrics != expected_metrics:
            print(f"client.get('k2') returned wrong result. Waiting: "
                  f"{expected_metrics}. Received: {metrics}")
            sys.exit(1)
    except Exception as err:
        print(f"Error when calling client.get('k2') {err.__class__}: {err}")
        sys.exit(1)


def test_get_key2():
    try:
        result = client1.get("k3")
        if result != {}:
            print(
                f"Error when getting the key that does not exist"
                f"Waiting: empty dict. Received: {result}")
            sys.exit(1)
    except Exception as err:
        print(f"Error!"
              f"{err.__class__} {err}")
        sys.exit(1)
