import pytest

from ansq import ConnectionFeatures, ConnectionOptions, open_connection
from ansq.tcp.types import NSQCommands


async def test_unix_socket_connection_success(nsqd_with_unix_sockets):
    nsq = await open_connection("/tmp/nsqd.sock")
    assert nsq.status.is_connected

    await nsq.close()
    assert nsq.status.is_closed


async def test_unix_socket_reconnect_after_close(nsqd_with_unix_sockets):
    nsq = await open_connection("/tmp/nsqd.sock")
    assert nsq.status.is_connected

    await nsq.close()
    assert nsq.status.is_closed

    assert await nsq.reconnect()
    assert nsq.status.is_connected

    await nsq.close()
    assert nsq.status.is_closed


async def test_unix_socket_reconnect_while_connected(nsqd_with_unix_sockets):
    nsq = await open_connection("/tmp/nsqd.sock")
    assert nsq.status.is_connected

    assert await nsq.reconnect()
    assert nsq.status.is_connected

    await nsq.close()
    assert nsq.status.is_closed


async def test_unix_socket_auto_reconnect(nsqd_with_unix_sockets, wait_for):
    nsq = await open_connection(
        "/tmp/nsqd.sock",
        connection_options=ConnectionOptions(auto_reconnect=True)
    )
    assert nsq.status.is_connected

    await nsqd_with_unix_sockets.stop()
    await wait_for(lambda: nsq.status.is_reconnecting)

    await nsqd_with_unix_sockets.start()
    await wait_for(lambda: nsq.status.is_connected)

    await nsq.close()
    assert nsq.status.is_closed


async def test_unix_socket_invalid_feature(create_nsqd_with_unix_sockets, wait_for, nsqd_with_unix_sockets):
    nsq = await open_connection(
        "/tmp/nsqd.sock",
        connection_options=ConnectionOptions(
            # Default max heartbeat is 60s
            features=ConnectionFeatures(heartbeat_interval=60001)
        )
    )
    assert nsq.status.is_closed


async def test_unix_socket_connection_options_as_kwargs(nsqd_with_unix_sockets):
    nsq = await open_connection("/tmp/nsqd.sock", debug=True)
    assert nsq._options.debug is True
    await nsq.close()


async def test_unix_socket_feature_options_as_kwargs(nsqd_with_unix_sockets):
    nsq = await open_connection("/tmp/nsqd.sock", heartbeat_interval=30001)
    assert nsq._options.features.heartbeat_interval == 30001
    await nsq.close()


async def test_unix_socket_invalid_kwarg(nsqd_with_unix_sockets):
    with pytest.raises(
        TypeError, match="got an unexpected keyword argument: 'invalid_kwarg'"
    ):
        await open_connection("/tmp/nsqd.sock", invalid_kwarg=1)


@pytest.mark.parametrize(
    "cmd", (NSQCommands.RDY, NSQCommands.FIN, NSQCommands.TOUCH, NSQCommands.REQ)
)
async def test_unix_socket_errors_from_commands_without_responses(nsqd_with_unix_sockets, wait_for, cmd, caplog):
    nsq = await open_connection("/tmp/nsqd.sock")

    response = await nsq.execute(cmd)
    await wait_for(lambda: caplog.messages)
    await nsq.close()

    expected_log = f"[E_INVALID] cannot {cmd.decode('utf8')} in current state"
    assert expected_log in caplog.text
    assert response is None
