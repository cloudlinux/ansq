import asyncio

import ansq


async def main():
    writer = await ansq.create_writer(
        nsqd_tcp_addresses=["/var/run/nsqd.sock"],
    )
    await writer.pub(
        topic="example_topic",
        message="Hello, world!",
    )
    await writer.close()


if __name__ == "__main__":
    asyncio.run(main())
