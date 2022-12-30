import pytest

from ansq import create_reader, open_connection
from ansq.tcp.reader import Reader


@pytest.fixture
async def nsqd_with_unix_sockets2(tmp_path, create_nsqd_with_unix_sockets):
    async with create_nsqd_with_unix_sockets(addr="/tmp/nsqd.sock2", http_addr="/tmp/nsqd-http.sock2") as nsqd:
        yield nsqd


async def test_unix_socket_create_reader(nsqd_with_unix_sockets):
    reader = await create_reader(
        nsqd_tcp_addresses=["/tmp/nsqd.sock"],
        topic="foo", channel="bar")

    assert reader.topic == "foo"
    assert reader.channel == "bar"
    assert reader.max_in_flight == 1

    await reader.close()


async def test_unix_socket_connect_reader(nsqd_with_unix_sockets):
    reader = Reader(
        nsqd_tcp_addresses=["/tmp/nsqd.sock"],
        topic="foo", channel="bar")
    assert not reader.connections

    await reader.connect()
    open_connections = [conn for conn in reader.connections if conn.is_connected]
    assert len(open_connections) == 1

    await reader.close()


async def test_unix_socket_close_reader(nsqd_with_unix_sockets):
    reader = await create_reader(
        nsqd_tcp_addresses=["/tmp/nsqd.sock"],
        topic="foo", channel="bar")
    await reader.close()

    closed_connections = [conn for conn in reader.connections if conn.is_closed]
    assert len(closed_connections) == 1


async def test_unix_socket_wait_for_message(nsqd_with_unix_sockets):
    nsq = await open_connection(nsqd_with_unix_sockets.tcp_address)
    await nsq.pub(topic="foo", message="test_unix_socket_message")
    await nsq.close()

    reader = await create_reader(
        nsqd_tcp_addresses=["/tmp/nsqd.sock"],
        topic="foo", channel="bar")

    message = await reader.wait_for_message()
    await message.fin()
    assert message.body == b"test_unix_socket_message"

    await reader.close()


async def test_unix_socket_messages_generator(nsqd_with_unix_sockets):
    nsq = await open_connection(nsqd_with_unix_sockets.tcp_address)
    await nsq.pub(topic="foo", message="test_unix_socket_message1")
    await nsq.pub(topic="foo", message="test_unix_socket_message2")
    await nsq.close()

    reader = await create_reader(
        nsqd_tcp_addresses=["/tmp/nsqd.sock"],
        topic="foo", channel="bar")

    read_messages = []
    async for message in reader.messages():
        read_messages.append(message.body.decode())
        await message.fin()
        if len(read_messages) >= 2:
            break

    assert read_messages == ["test_unix_socket_message1", "test_unix_socket_message2"]

    await reader.close()


async def test_unix_socket_read_from_multiple_tcp_addresses(nsqd_with_unix_sockets, nsqd_with_unix_sockets2):
    reader = await create_reader(
        topic="foo",
        channel="bar",
        nsqd_tcp_addresses=[nsqd_with_unix_sockets.tcp_address, nsqd_with_unix_sockets2.tcp_address],
    )

    nsq1 = await open_connection(nsqd_with_unix_sockets.tcp_address)
    await nsq1.pub(topic="foo", message="test_unix_socket_message1")
    await nsq1.close()

    message = await reader.wait_for_message()
    await message.fin()
    assert message.body == b"test_unix_socket_message1"

    nsq2 = await open_connection(nsqd_with_unix_sockets2.tcp_address)
    await nsq2.pub(topic="foo", message="test_unix_socket_message2")
    await nsq2.close()

    message = await reader.wait_for_message()
    await message.fin()
    assert message.body == b"test_unix_socket_message2"

    await reader.close()
