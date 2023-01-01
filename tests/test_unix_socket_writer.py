import pytest

from ansq import create_reader, create_writer
from ansq.tcp.writer import Writer


@pytest.fixture
async def nsqd_with_unix_sockets2(tmp_path, create_nsqd_with_unix_sockets):
    async with create_nsqd_with_unix_sockets(addr="/tmp/nsqd.sock2", http_addr="/tmp/nsqd-http.sock2") as nsqd:
        yield nsqd


async def test_unix_socket_create_writer(nsqd_with_unix_sockets):
    writer = await create_writer(nsqd_tcp_addresses=["/tmp/nsqd.sock"])

    open_connections = [conn for conn in writer.connections if conn.is_connected]
    assert len(open_connections) == 1

    await writer.close()


async def test_unix_socket_connect_writer(nsqd_with_unix_sockets):
    writer = Writer(nsqd_tcp_addresses=["/tmp/nsqd.sock"])
    assert not writer.connections

    await writer.connect()
    open_connections = [conn for conn in writer.connections if conn.is_connected]
    assert len(open_connections) == 1

    await writer.close()


async def test_unix_socket_close_writer(nsqd_with_unix_sockets):
    writer = await create_writer(nsqd_tcp_addresses=["/tmp/nsqd.sock"])
    await writer.close()

    closed_connections = [conn for conn in writer.connections if conn.is_closed]
    assert len(closed_connections) == 1


async def test_unix_socket_pub(nsqd_with_unix_sockets):
    writer = await create_writer(nsqd_tcp_addresses=["/tmp/nsqd.sock"])

    response = await writer.pub(topic="foo", message="test_unix_socket_message")
    assert response.is_ok

    await writer.close()


async def test_unix_socket_mpub(nsqd_with_unix_sockets):
    writer = await create_writer(nsqd_tcp_addresses=["/tmp/nsqd.sock"])

    messages = [f"test_unix_socket_message_{i}" for i in range(10)]
    response = await writer.mpub("foo", *messages)
    assert response.is_ok

    await writer.close()


async def test_unix_socket_dpub(nsqd_with_unix_sockets):
    writer = await create_writer(nsqd_tcp_addresses=["/tmp/nsqd.sock"])

    response = await writer.dpub(topic="foo", message="test_unix_socket_message", delay_time=1)
    assert response.is_ok

    await writer.close()


async def test_unix_socket_pub_to_multiple_tcp_addresses(nsqd_with_unix_sockets, nsqd_with_unix_sockets2):
    writer = await create_writer(
        nsqd_tcp_addresses=[nsqd_with_unix_sockets.tcp_address,
                            nsqd_with_unix_sockets2.tcp_address],
    )

    response = await writer.pub(topic="foo", message="test_unix_socket_message")
    assert response.is_ok

    await writer.close()

    reader = await create_reader(
        topic="foo",
        channel="bar",
        nsqd_tcp_addresses=[nsqd_with_unix_sockets.tcp_address,
                            nsqd_with_unix_sockets2.tcp_address],
    )

    message = await reader.wait_for_message()
    assert message.body == b"test_unix_socket_message"

    await reader.close()
