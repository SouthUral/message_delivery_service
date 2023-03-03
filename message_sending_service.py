import asyncio
from loguru import logger
import aioamqp

# подключение к RabitMQ по URL
def url() -> str:
    return f"{'amqp'}://" \
           f"{config['user']}:" \
           f"{get_decrypted(config['password'])}@" \
           f"{env.rmq_host}:{env.rmq_port}/{env.rmq_vhost}"

transport, protocol = None, None
# функция коннекта к RabitMQ
async def connect(transport, protocol):
    try:
        transport, protocol = await aioamqp.from_url(url=url(), program_name="algo", heartbeat=5)
        logger.success("RabbitMQ connected!")
    except aioamqp.AmqpClosedConnection as _err:
        logger.error(f"RabbitMQ connection error: {_err}")

await connect(transport, protocol)

channel = await protocol.channel()
await self.channel.exchange(exchange_name=config['exchanges']['arrival'], type_name='direct', durable=True, auto_delete=False)


async def _call(self, exchange: str, message):
    if self.channel is None or not self.channel.is_open:
        await self.connect()
    try:
        result = await asyncio.wait_for(self.channel.publish(message, exchange_name=exchange, routing_key=''), timeout=0.5)
        logger.debug(f"RabbitMQ [x] Sent call result: {result}, message: {message}")
    except asyncio.TimeoutError:
        logger.error('RabbitMQ call message timeout!')
    except aioamqp.exceptions.AmqpClosedConnection as _err:
        logger.error(f"RabbitMQ call message error: AmqpClosedConnection")
        self.channel = None

async def events_stream(self, callback):
    if self.channel is None or not self.channel.is_open:
        await self.connect()
    await self.channel.basic_qos(prefetch_count=100)
    await self.channel.basic_consume(
        callback=callback, 
        queue_name='iLogic.Events',
        no_ack=False,
        arguments={
            "x-stream-offset": "first"
        })

async def call_command(self, message):
    await self._call(config['exchanges']['commands'], message)

async def call_arrival(self, message):
    await self._call(config['exchanges']['arrival'], message)

async def close(self):
    await self.protocol.close()
    self.transport.close()

async def create_connects():
    """
    Запуск задач с алгоритмом
    """
    await asyncio.gather(
        cmd.pdb.connect(),
        mdb.client.connect(),
        # routes.create_session(),
        rpcController.client.create(),
        mq.rmqClient.connect(),
        mq.rmqClient.events_stream(get_events)
    )

loop.run_until_complete(create_connects())