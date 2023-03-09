import asyncio
import aioamqp
import asyncpg
import json
from loguru import logger


def url_rb():
    port = "5672"
    user = "guest"
    password = "guest"
    v_host = "new"
    host = "localhost"
    return f"amqp://{user}:{password}@{host}:{port}/"

def url_db():
    port = "54320"
    user = "admin"
    password = "1234567"
    host = "127.0.0.1"
    database = "db01"
    return f"postgresql://{user}:{password}@{host}:{port}/{database}"


class MessageTransmitter:
    def __init__(self, url_rabbit: str, url_db: str):
        self.url_rabbit = url_rabbit
        self.url_db = url_db
        self.offset = "last"
        self.transport = None
        self.protocol = None
        self.channel = None
        self.bd_conn = None

    async def _connect_bd(self):
        self.bd_conn = await asyncpg.connect(self.url_db)
        logger.success(f"Connected to PostgresQL")

    async def _connect_rb(self):
        try:
            self.transport, self.protocol = await aioamqp.from_url(url=self.url_rabbit)
            logger.success("RabbitMQ connected!")
        except aioamqp.AmqpClosedConnection as _err:
            logger.error(f"RabbitMQ connection error: {_err}")

    async def _check_offset(self):
        last_value_db = await self.bd_conn.fetchrow('''select 
                            offset_number
                        from 
                            sh_signal.test
                        order by offset_number desc limit(1)
                        ;''')   
        if last_value_db:
            self.offset = last_value_db['offset_number'] + 1

    async def _events_stream(self):
        self.channel = await self.protocol.channel()
        await self._check_offset()
        await self.channel.basic_qos(prefetch_count=100)
        await self.channel.basic_consume(
            callback=self._callback,
            queue_name="new",
            arguments={
                "x-stream-offset": self.offset
            })
        await asyncio.sleep(1)
        logger.success("Сообщения прочитаны")

    async def _callback(self, channel, body, envelope, properties):
        offset = properties.headers["x-stream-offset"]
        message = json.dumps({'message': body.decode(), 'offset': int(offset)})
        await self.bd_conn.execute('''call sh_signal.calc_quality($1)''', message)
        logger.success(f"The message has been sent")
        print(" [x] Received {} - {}".format(body, offset))
        await channel.basic_client_ack(delivery_tag=envelope.delivery_tag)

    async def _close(self):
        await self.protocol.close()
        self.transport.close()
        logger.success("RabbitMQ connect closed!")
        await self.connect.close()
        logger.success(f"Close connect to PostgresQL")

    async def __call__(self, *args, **kwds):
        await self._connect_bd()
        await self._connect_rb()
        await self._events_stream()


mt = MessageTransmitter(url_rabbit=url_rb(), url_db=url_db())
mt()

loop = asyncio.get_event_loop()
loop.run_until_complete(mt())
loop.run_forever()