import asyncio

from ansq.http import NSQDHTTPWriter


async def main():
    writer = NSQDHTTPWriter()
    print(await writer.ping())
    print(await writer.info())
    print(await writer.stats())
    print(await writer.pub(topic="example_topic", message="Hello, world!"))
    print(await writer.mpub("example_topic", *["Hello, world!"] * 10))


if __name__ == "__main__":
    asyncio.run(main())
