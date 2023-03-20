import os
import json
import toml
import asyncio
import aioamqp
import asyncpg
import socket
import dataclasses

from loguru import logger
from cryptography.fernet import Fernet
from asyncpg.exceptions import InterfaceError


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
    def __init__(self, url_rabbit: url_key, url_db: url_key):
        self.url_rabbit = url_rabbit
        self.url_db = url_db
        self.offset = "last"
        self.transport = None
        self.protocol = None
        self.channel = None
        self.bd_conn = None

    async def _connect_bd(self):
        try:
            self.bd_conn = await asyncpg.connect(self.url_db())
            logger.success(f"Connected to PostgresQL")
        except OSError as err:
            logger.error(f"connect_bd error: {err}")
            asyncio.get_event_loop().stop()

    async def _connect_rb(self):
        try:
            self.transport, self.protocol = await aioamqp.connect(
                host=self.url_rabbit.host,
                port=self.url_rabbit.port,
                login=self.url_rabbit.user,
                password=self.url_rabbit.password,
                virtualhost=self.url_rabbit.spec_param,
                on_error=self.error_callback,
                client_properties={
                    "heartbeat": config.get('rabbit', {}).get('heartbeat'), 
                    "program_name": config['app'].get('name'),
                    'hostname' : socket.gethostname(),
                },
            )
            self.channel = await self.protocol.channel()
            logger.success("RabbitMQ connected!")
        except aioamqp.AmqpClosedConnection as err:
            logger.error("RabbitMQ connection error:", err)
        except OSError as err:
            logger.error("RabbitMQ connection error:", err)
            asyncio.get_event_loop().stop()

    async def error_callback(self, exception):
        logger.error(f"RabbitMQ error: {exception}")
        asyncio.get_event_loop().stop()

    async def _check_offset(self):
        last_value_db = await self.bd_conn.fetchrow(
            "select sh_signal.get_quality_offset() as queue_offset"
        )
        if last_value_db:
            if last_value_db["queue_offset"]:
                self.offset = last_value_db["queue_offset"] + 1
        logger.info(f"Offset:{self.offset}")

    async def _events_stream(self):
        try:
            await self._check_offset()
            await self.channel.basic_qos(prefetch_count=1)
            await self.channel.basic_consume(
                callback=self._callback,
                queue_name=config.get('rabbit', {}).get('name_queue'),
                arguments={"x-stream-offset": self.offset},
            )
            logger.success("RabbitMQ init complete")
        except Exception as _exc:
            logger.exception(_exc)
            asyncio.get_event_loop().stop()

    async def _callback(self, channel, body, envelope, properties):
        offset = properties.headers["x-stream-offset"]
        # message = json.dumps({"message": body.decode(), "offset": int(offset)})
        message = json.loads(body)
        logger.success(f"[{offset}] messages received")
        await channel.basic_client_ack(delivery_tag=envelope.delivery_tag)
        try:
            if 'data' in message:
                await self.bd_conn.execute("call sh_signal.calc_quality($1, $2)", json.dumps(message['data']), offset )
                logger.success(f"[{offset}] messages are sent to the database")
        except InterfaceError as err:
            logger.error(err)
            asyncio.get_event_loop().stop()

    async def _close(self):
        await self.protocol.close()
        self.transport.close()
        logger.success("RabbitMQ connect closed!")
        await self.connect.close()
        logger.success(f"Close connect to PostgresQL")

    async def __call__(self, *args, **kwds):
        await asyncio.gather(self._connect_bd(), self._connect_rb())
        await self._events_stream()


def get_decrypted(text):
    return Fernet(os.getenv('ASD_CIPHER_KEY').encode()).decrypt(text.encode()).decode()


if __name__ == "__main__":
    config = toml.load('./config.toml')
    url_rb = url_key(port=os.getenv('ASD_RMQ_PORT'),
                user=config.get('rabbit', {}).get('username'), 
                password=get_decrypted(config.get('rabbit', {}).get('password')), 
                connection="amqp",
                host=os.getenv('ASD_RMQ_HOST'),
                spec_param=os.getenv('ASD_RMQ_VHOST')
                )
    
    url_db = url_key(port=os.getenv('ASD_POSTGRES_PORT'), 
                user=config.get('postgres', {}).get('username'), 
                password=get_decrypted(config.get('postgres', {}).get('password')), 
                spec_param=os.getenv('ASD_POSTGRES_DBNAME'),
                host=os.getenv('ASD_POSTGRES_HOST')
                )
    mt = MessageTransmitter(url_rabbit=url_rb, url_db=url_db)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(mt())
    loop.run_forever()
