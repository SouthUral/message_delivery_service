import asyncio
import aioamqp
from asyncpg.exceptions import InterfaceError
import asyncpg
import json
from loguru import logger
import dataclasses


@dataclasses.dataclass
class url_key:
    port: str = "5432"
    user: str = "user"
    password: str = "password"
    host: str = "localhost"
    spec_param: str = "postgres"
    connection: str = "postgresql"

    def __call__(self) -> str: 
        return f"{self.connection}://{self.user}:{self.password}@{self.host}:{self.port}/{self.spec_param}"


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
        try:
            self.bd_conn = await asyncpg.connect(self.url_db)
            logger.success(f"Connected to PostgresQL")
        except OSError as err:
            logger.error(f"connect_bd error: {err}")
            asyncio.get_event_loop().stop()

    async def _connect_rb(self):
        try:
            url = aioamqp.urlparse(url=self.url_rabbit)
            self.transport, self.protocol = await aioamqp.connect(
                host=url.hostname,
                port=url.port,
                login=url.username,
                password=url.password,
                on_error=self.error_callback,
                client_properties={'heartbeat': 10,
                                   'program_name': 'some'}
            )
            self.channel = await self.protocol.channel()
            logger.success("RabbitMQ connected!")
        except aioamqp.AmqpClosedConnection as err:
            logger.error(f"RabbitMQ connection error: {err}")
        except OSError as err:
            logger.error(f"RabbitMQ connection error: {err}")
            asyncio.get_event_loop().stop()

    async def error_callback(self, exception):
        logger.error(f"RabbitMQ error: {exception}")
        asyncio.get_event_loop().stop()


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
        try:
            await self._check_offset()
            await self.channel.basic_qos(prefetch_count=1)
            await self.channel.basic_consume(
                callback=self._callback,
                queue_name="new",
                arguments={
                    "x-stream-offset": self.offset
                },)
            logger.success("Сообщения прочитаны")
        except:
            logger.error(f"events_stream error")

    async def _callback(self, channel, body, envelope, properties):
        offset = properties.headers["x-stream-offset"]
        message = json.dumps({'message': body.decode(), 'offset': int(offset)})
        logger.success(f"messages received offset:{offset}")
        await channel.basic_client_ack(delivery_tag=envelope.delivery_tag)
        try:
            await self.bd_conn.execute('''call sh_signal.calc_quality($1)''', message)
            logger.success("messages are sent to the database")
        except InterfaceError as err:
            logger.error(f"error callback 2: {err}")
            asyncio.get_event_loop().stop()

    async def _close(self):
        await self.protocol.close()
        self.transport.close()
        logger.success("RabbitMQ connect closed!")
        await self.connect.close()
        logger.success(f"Close connect to PostgresQL")

    async def __call__(self, *args, **kwds):
        await asyncio.gather(
            self._connect_bd(),
            self._connect_rb()
            )
        await self._events_stream()

if __name__ == "__main__":
    url_rb = url_key(port="5672", user="guest", password="guest", connection="amqp", spec_param="")
    url_db = url_key(port="54320", user="admin", password="1234567", spec_param="db01")
    mt = MessageTransmitter(url_rabbit=url_rb(), url_db=url_db())
    loop = asyncio.get_event_loop()
    loop.run_until_complete(mt())
    loop.run_forever()
